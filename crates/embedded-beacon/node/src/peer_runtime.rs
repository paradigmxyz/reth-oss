//! Persistent checkpoint-derived peer startup, deliberately separate from live node readiness.
//!
//! Storage and SSZ work are blocking. A future import coordinator must construct this component
//! on a blocking task, then drive it from one runtime owner. Peer observations confer no import
//! authority. Sockets require compiled readiness or the explicit canonical Hoodi checkpoint
//! experiment constructed from the same independently authenticated import owner.

use crate::{bootstrap::checked_config_digest, BootstrapError, Readiness, VerifiedCheckpoint};
use discv5::{
    enr::{CombinedKey, EnrKey},
    Enr, ListenConfig,
};
use libp2p::{Multiaddr, PeerId};
use reth_embedded_beacon_config::{ChainConfig, ConfigError};
use reth_embedded_beacon_network::{
    availability_rpc::{SidecarRequest, WireSidecar},
    block_rpc::{BlockRequest, WireBlock},
    discovery::{sign_local_enr, DiscoveryError, DiscoveryRecord, LocalAdvertisement},
    envelope_rpc::{EnvelopeBlockReference, EnvelopeRequest, WireEnvelope},
    peer::{PeerRuntime, PeerRuntimeError, PeerRuntimeEvent, TrustedStatusSnapshot},
    transport::{BlockRequestId, EnvelopeRequestId, SidecarRequestId},
    DiscoveryCompatibility, EndpointPolicy, Status, StatusV2,
};
use reth_embedded_beacon_primitives::{sha256, Root, Slot};
use reth_embedded_beacon_shared_data::{journal::publish_file, StoreError, StoreIdentity};
use reth_fs_util as fs;
use std::{
    fmt,
    fs::{File, OpenOptions},
    io::Read,
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
};
use zeroize::Zeroizing;

const MAGIC: &[u8; 8] = b"EBPEER01";
const HEADER_BYTES: usize = 159;
const MAX_IDENTITY_BYTES: usize = 4096;
const MAX_DIRECTORY_ENTRIES: usize = 16;

/// Explicit advertised and optional local bind addresses; no remote discovery is inferred.
#[derive(Clone, Copy, Debug)]
pub struct PeerListenConfig {
    /// One literal advertised IP authenticated by the signed ENR routing policy.
    pub ip: IpAddr,
    /// Local interface or wildcard binding; defaults to the advertised IP when absent.
    pub bind_ip: Option<IpAddr>,
    /// Dedicated discovery UDP port, distinct from QUIC.
    pub discovery_port: u16,
    /// TCP/Noise listener port.
    pub tcp_port: u16,
    /// QUIC/TLS UDP listener port.
    pub quic_port: u16,
    /// Explicit public routing policy or isolated loopback testing policy.
    pub policy: EndpointPolicy,
}

impl PeerListenConfig {
    fn validate(self) -> Result<(), PeerStartupError> {
        let bind_ip = self.bind_ip.unwrap_or(self.ip);
        if !self.policy.permits(self.ip) ||
            !self.policy.permits_bind(bind_ip) ||
            bind_ip.is_ipv4() != self.ip.is_ipv4() ||
            self.discovery_port == 0 ||
            self.tcp_port == 0 ||
            self.quic_port == 0 ||
            self.discovery_port == self.quic_port
        {
            return Err(PeerStartupError::Configuration);
        }
        Ok(())
    }

    const fn advertisement(self) -> LocalAdvertisement {
        LocalAdvertisement {
            discovery_socket: SocketAddr::new(self.ip, self.discovery_port),
            tcp_port: self.tcp_port,
            quic_port: self.quic_port,
            attnets: [0; 8],
            syncnets: 0,
            custody_group_count: 0,
        }
    }

    const fn discovery_listener(self) -> ListenConfig {
        let bind_ip = match self.bind_ip {
            Some(ip) => ip,
            None => self.ip,
        };
        match bind_ip {
            IpAddr::V4(ip) => ListenConfig::Ipv4 { ip, port: self.discovery_port },
            IpAddr::V6(ip) => ListenConfig::Ipv6 { ip, port: self.discovery_port },
        }
    }

    fn transport_listeners(self) -> Result<[Multiaddr; 2], PeerStartupError> {
        let ip = match self.bind_ip.unwrap_or(self.ip) {
            IpAddr::V4(ip) => format!("/ip4/{ip}"),
            IpAddr::V6(ip) => format!("/ip6/{ip}"),
        };
        Ok([
            format!("{ip}/tcp/{}", self.tcp_port)
                .parse()
                .map_err(|_| PeerStartupError::Configuration)?,
            format!("{ip}/udp/{}/quic-v1", self.quic_port)
                .parse()
                .map_err(|_| PeerStartupError::Configuration)?,
        ])
    }
}

/// Startup failures without private keys, signed record contents or remote error strings.
#[derive(Debug, thiserror::Error)]
pub enum PeerStartupError {
    /// A checkpoint was not authenticated by the concrete, identically configured verifier.
    #[error("peer checkpoint lacks the exact verified consensus configuration binding")]
    UnverifiedCheckpoint,
    /// Original genesis and sealed graph-origin evidence do not bind this exact network.
    #[error("peer genesis lacks exact sealed graph origin binding")]
    UnverifiedGenesis,
    /// Concrete genesis original authentication failed.
    #[error(transparent)]
    #[cfg(feature = "gossip-validation")]
    Genesis(#[from] crate::GenesisError),
    /// A profile, clock, endpoint or durable record disagrees with the expected configuration.
    #[error("invalid peer startup configuration")]
    Configuration,
    /// Network configuration validation failed.
    #[error(transparent)]
    Config(#[from] ConfigError),
    /// Current time regresses behind an authenticated or persisted slot.
    #[error("peer startup clock regressed")]
    ClockRegression,
    /// Persisted key, checksum, signature, sequence or binary encoding is invalid.
    #[error("corrupt peer identity snapshot")]
    CorruptIdentity,
    /// Input file or directory exceeds the small identity storage budget.
    #[error("peer identity storage exceeds its bounded limit")]
    IdentityTooLarge,
    /// Signed record changed without a strictly increasing sequence.
    #[error("peer identity sequence regressed")]
    SequenceRegression,
    /// The component must be explicitly admitted and started before peer IO.
    #[error("checkpoint peer runtime is dormant")]
    Dormant,
    /// The explicit checkpoint experiment does not match canonical Hoodi public routing.
    #[error(
        "experimental peer startup requires a canonical Hoodi checkpoint owner and public routing"
    )]
    ExperimentalAdmission,
    /// Full implementation readiness or concrete bootstrap validation failed.
    #[error(transparent)]
    Bootstrap(#[from] BootstrapError),
    /// Atomic durable publication failed; uncertain publication requires reopen.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// Bounded filesystem operation failed.
    #[error(transparent)]
    Fs(#[from] fs::FsPathError),
    /// Signed discovery validation or scheduling failed.
    #[error(transparent)]
    Discovery(#[from] DiscoveryError),
    /// Authenticated transport operation failed.
    #[error(transparent)]
    Peer(#[from] PeerRuntimeError),
}

/// One dormant peer owner anchored to a concretely verified checkpoint.
/// Clock updates can change fork advertisements, but cannot invent a new head or finality.
pub struct CheckpointPeerRuntime {
    config: ChainConfig,
    current_slot: Slot,
    trusted_status: TrustedStatusSnapshot,
    listeners: PeerListenConfig,
    identity: IdentityStore,
    peer: PeerRuntime,
    started: bool,
    genesis_origin: Option<Root>,
    // Set only after the distinct experimental constructor consumes sealed checkpoint status.
    experimental_sync: bool,
}

impl fmt::Debug for CheckpointPeerRuntime {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CheckpointPeerRuntime")
            .field("current_slot", &self.current_slot)
            .field("started", &self.started)
            .field("genesis_origin", &self.genesis_origin)
            .finish_non_exhaustive()
    }
}

impl CheckpointPeerRuntime {
    /// Recover or durably generate one identity under `repository_dir/peer-identity`.
    /// No sockets are opened. The profile digest must bind all private execution anchors/settings;
    /// the concrete checkpoint's separate digest binds the complete consensus configuration.
    pub fn open(
        repository_dir: impl AsRef<Path>,
        identity: StoreIdentity,
        config: ChainConfig,
        checkpoint: VerifiedCheckpoint,
        now_unix_ms: u64,
        listeners: PeerListenConfig,
    ) -> Result<Self, PeerStartupError> {
        Self::open_inner(repository_dir, identity, config, checkpoint, now_unix_ms, listeners, None)
    }

    fn open_inner(
        repository_dir: impl AsRef<Path>,
        identity: StoreIdentity,
        config: ChainConfig,
        checkpoint: VerifiedCheckpoint,
        now_unix_ms: u64,
        listeners: PeerListenConfig,
        trusted_status: Option<TrustedStatusSnapshot>,
    ) -> Result<Self, PeerStartupError> {
        let digest = checked_config_digest(&config)?;
        if checkpoint.config_digest() != Some(digest) {
            return Err(PeerStartupError::UnverifiedCheckpoint);
        }
        if identity.genesis_root != config.genesis_validators_root {
            return Err(PeerStartupError::Configuration);
        }
        let slot = config.clock()?.slot_at(now_unix_ms).ok_or(PeerStartupError::Configuration)?;
        let snapshot = match trusted_status {
            Some(trusted) => {
                if trusted.current_slot != slot ||
                    trusted.finalized.epoch < checkpoint.checkpoint().epoch
                {
                    return Err(PeerStartupError::UnverifiedCheckpoint)
                }
                trusted
            }
            None => snapshot(&config, &checkpoint, slot)?,
        };
        Self::open_status(repository_dir, identity, config, snapshot, now_unix_ms, listeners, None)
    }

    // Both entry points prove their distinct original evidence before using this private helper.
    // No public raw status, caller checkpoint or genesis boolean can enter this constructor.
    fn open_status(
        repository_dir: impl AsRef<Path>,
        identity: StoreIdentity,
        config: ChainConfig,
        snapshot: TrustedStatusSnapshot,
        now_unix_ms: u64,
        listeners: PeerListenConfig,
        genesis_origin: Option<Root>,
    ) -> Result<Self, PeerStartupError> {
        let digest = checked_config_digest(&config)?;
        let slot = config.clock()?.slot_at(now_unix_ms).ok_or(PeerStartupError::Configuration)?;
        if identity.genesis_root != config.genesis_validators_root || snapshot.current_slot != slot
        {
            return Err(PeerStartupError::Configuration)
        }
        listeners.validate()?;
        // Validate explicit advertisement before creating any identity files.
        let mut store =
            IdentityStore::open(repository_dir.as_ref(), identity, config.clone(), digest)?;
        let signed = sign_local_enr(
            &store.key,
            store.enr.as_ref().map_or(1, Enr::seq),
            &config,
            slot,
            &listeners.advertisement(),
            listeners.policy,
        )?;
        let signed = match &store.enr {
            Some(old) if !same_enr_content(old, &signed) => sign_local_enr(
                &store.key,
                old.seq().checked_add(1).ok_or(PeerStartupError::SequenceRegression)?,
                &config,
                slot,
                &listeners.advertisement(),
                listeners.policy,
            )?,
            Some(old) => old.clone(),
            None => signed,
        };
        store.persist(&signed, slot, listeners.policy)?;
        let peer = PeerRuntime::new(
            config.clone(),
            snapshot.clone(),
            signed,
            store.key_copy()?,
            listeners.discovery_listener(),
            listeners.policy,
        )?;
        tracing::info!(target: "beacon::peers", network = %config.name, slot, finalized_epoch = snapshot.finalized.epoch, genesis = genesis_origin.is_some(), "[beacon] Prepared dormant authenticated peer runtime");
        Ok(Self {
            config,
            current_slot: slot,
            trusted_status: snapshot,
            listeners,
            identity: store,
            peer,
            started: false,
            genesis_origin,
            experimental_sync: false,
        })
    }

    /// Exact concrete consensus configuration bound to the persisted authenticated peer owner.
    pub fn config_digest(&self) -> Result<Root, PeerStartupError> {
        self.identity.ensure_healthy()?;
        Ok(self.identity.digest)
    }

    /// Read the stable signed identity only while durable state is healthy.
    pub fn peer_id(&self) -> Result<PeerId, PeerStartupError> {
        self.identity.ensure_healthy()?;
        Ok(self.peer.peer_id())
    }

    /// Read the exact persisted local record; no secret key material is exposed.
    pub fn local_enr(&self) -> Result<Enr, PeerStartupError> {
        self.identity.ensure_healthy()?;
        Ok(self.peer.local_enr())
    }

    /// Current checkpoint-derived local advertisement. This cannot be supplied by an operator.
    pub fn status(&self) -> Result<StatusV2, PeerStartupError> {
        self.identity.ensure_healthy()?;
        Ok(self.trusted_status.status.clone())
    }

    /// Sign/persist changed fork fields before advertisement, using the validated network clock.
    /// Uncertain persistence closes all peer IO and requires dropping/reopening this component.
    pub fn advance_clock(&mut self, now_unix_ms: u64) -> Result<(), PeerStartupError> {
        self.identity.ensure_healthy()?;
        let slot =
            self.config.clock()?.slot_at(now_unix_ms).ok_or(PeerStartupError::Configuration)?;
        if slot < self.current_slot {
            return Err(PeerStartupError::ClockRegression);
        }
        let mut trusted = self.trusted_status.clone();
        trusted.current_slot = slot;
        trusted.status.status.fork_digest =
            self.config.fork_digest(slot / self.config.slots_per_epoch)?;
        let published = trusted.clone();
        let mut persistence_error = None;
        let store = &mut self.identity;
        let policy = self.listeners.policy;
        let result = self.peer.update_snapshot(trusted, |record| {
            store.persist(record, slot, policy).map_err(|error| {
                persistence_error = Some(error);
                DiscoveryError::Persistence
            })
        });
        if let Some(error) = persistence_error {
            self.shutdown();
            return Err(error);
        }
        result?;
        // The network callback runs only for changed ENR fields. Persist clock progress even
        // when the signed record stayed identical, so restart cannot regress a recorded clock.
        if let Err(error) = self.identity.persist(&self.peer.local_enr(), slot, policy) {
            self.shutdown();
            return Err(error);
        }
        if slot != self.current_slot {
            tracing::debug!(target: "beacon::peers", previous_slot = self.current_slot, slot, "[beacon] Persisted peer clock and fork advertisement");
        }
        self.current_slot = slot;
        self.trusted_status = published;
        Ok(())
    }

    /// Admit signed seed data without opening any sockets or bypassing startup readiness.
    pub fn add_seed(&self, text: &str) -> Result<(), PeerStartupError> {
        self.identity.ensure_healthy()?;
        self.peer.add_seed(text)?;
        Ok(())
    }

    /// Admit a signed discovery-only routing seed through the healthy dormant owner.
    /// Strict peer fork identity and status validation remain required after discovery.
    pub fn add_bootstrap_seed(&self, text: &str) -> Result<(), PeerStartupError> {
        self.identity.ensure_healthy()?;
        self.peer.add_bootstrap_seed(text)?;
        Ok(())
    }

    /// Require compiled readiness or the distinctly authenticated Hoodi experiment.
    /// Neither route overrides identity health, transport admission or consensus validation.
    pub async fn start(&mut self) -> Result<(), PeerStartupError> {
        self.identity.ensure_healthy()?;
        if self.experimental_sync {
            validate_experimental_admission(&self.config, self.listeners, self.genesis_origin)?;
        } else {
            Readiness::current().require_live_sync()?;
        }
        if self.started {
            return Ok(());
        }
        for address in self.listeners.transport_listeners()? {
            if let Err(error) = self.peer.listen_on(address) {
                self.shutdown();
                return Err(error.into());
            }
        }
        if let Err(error) = self.peer.start_discovery().await {
            self.shutdown();
            return Err(error.into());
        }
        self.started = true;
        tracing::info!(target: "beacon::peers", tcp_port = self.listeners.tcp_port, quic_port = self.listeners.quic_port, discovery_port = self.listeners.discovery_port, "[beacon] Started checkpoint peer listeners");
        Ok(())
    }

    /// Request one bounded UDP discovery query only after compiled startup admission.
    pub fn request_discovery(&mut self) -> Result<(), PeerStartupError> {
        self.identity.ensure_healthy()?;
        if !self.started {
            return Err(PeerStartupError::Dormant);
        }
        self.peer.request_discovery()?;
        Ok(())
    }

    /// Retrieve signed blocks after persisted identity and live-start admission.
    /// Checked peer status and transport byte/request limits remain enforced by the peer owner.
    pub fn request_blocks(
        &mut self,
        peer: PeerId,
        request: BlockRequest,
    ) -> Result<BlockRequestId, PeerStartupError> {
        self.ensure_started()?;
        Ok(self.peer.request_blocks(peer, request)?)
    }

    /// Resolve the exact original delivery after native consensus validation.
    pub fn report_gossip(
        &mut self,
        id: reth_embedded_beacon_network::transport::GossipRequestId,
        verdict: reth_embedded_beacon_network::transport::GossipVerdict,
    ) -> Result<bool, PeerStartupError> {
        Ok(self.peer.report_gossip(id, verdict)?)
    }

    /// Serve retained signed blocks only through the original private response token.
    pub fn respond_blocks(
        &mut self,
        id: BlockRequestId,
        blocks: Vec<WireBlock>,
    ) -> Result<(), PeerStartupError> {
        self.ensure_started()?;
        Ok(self.peer.respond_blocks(id, blocks)?)
    }

    /// Report unavailable block history without manufacturing an imported block.
    pub fn respond_blocks_unavailable(
        &mut self,
        id: BlockRequestId,
    ) -> Result<(), PeerStartupError> {
        self.ensure_started()?;
        Ok(self.peer.respond_blocks_unavailable(id)?)
    }

    /// Retrieve Gloas envelopes with small metadata copied from their original decoded blocks.
    /// Dropping the original block batch before this call releases the shared bulk read budget.
    pub fn request_envelopes(
        &mut self,
        peer: PeerId,
        request: EnvelopeRequest,
        blocks: Vec<EnvelopeBlockReference>,
    ) -> Result<EnvelopeRequestId, PeerStartupError> {
        self.ensure_started()?;
        Ok(self.peer.request_envelopes(peer, request, blocks)?)
    }

    /// Serve retained envelopes bound to actual decoded block roots, slots and context digests.
    pub fn respond_envelopes(
        &mut self,
        id: EnvelopeRequestId,
        envelopes: Vec<WireEnvelope>,
        blocks: &[EnvelopeBlockReference],
    ) -> Result<(), PeerStartupError> {
        self.ensure_started()?;
        Ok(self.peer.respond_envelopes(id, envelopes, blocks)?)
    }

    /// Report unavailable payload history through the existing authenticated response token.
    pub fn respond_envelopes_unavailable(
        &mut self,
        id: EnvelopeRequestId,
    ) -> Result<(), PeerStartupError> {
        self.ensure_started()?;
        Ok(self.peer.respond_envelopes_unavailable(id)?)
    }

    /// Retrieve original candidate-bound sidecars after concrete startup admission.
    /// Wire decoding grants no mathematical availability or consensus authority.
    pub fn request_sidecars(
        &mut self,
        peer: PeerId,
        request: SidecarRequest,
    ) -> Result<SidecarRequestId, PeerStartupError> {
        self.ensure_started()?;
        Ok(self.peer.request_sidecars(peer, request)?)
    }

    /// Serve original retained sidecars through the matching private inbound response token.
    pub fn respond_sidecars(
        &mut self,
        id: SidecarRequestId,
        sidecars: Vec<WireSidecar>,
    ) -> Result<(), PeerStartupError> {
        self.ensure_started()?;
        Ok(self.peer.respond_sidecars(id, sidecars)?)
    }

    /// Report unavailable data without granting candidate availability.
    pub fn respond_sidecars_unavailable(
        &mut self,
        id: SidecarRequestId,
    ) -> Result<(), PeerStartupError> {
        self.ensure_started()?;
        Ok(self.peer.respond_sidecars_unavailable(id)?)
    }

    /// Drive bounded authenticated peer observations; none authorizes consensus import.
    pub async fn next_event(&mut self) -> Result<PeerRuntimeEvent, PeerStartupError> {
        self.identity.ensure_healthy()?;
        if !self.started {
            return Err(PeerStartupError::Dormant);
        }
        let event = self.peer.next_event().await?;
        match &event {
            PeerRuntimeEvent::DiscoveryComplete { candidate_count } => {
                tracing::debug!(target: "beacon::peers", candidate_count, "[beacon] Completed peer discovery query");
            }
            PeerRuntimeEvent::CandidateRejected { peer, reason } => {
                tracing::debug!(target: "beacon::peers", %peer, %reason, "[beacon] Peer candidate rejected by transport admission");
            }
            PeerRuntimeEvent::Transport(_) | PeerRuntimeEvent::Tick => {}
        }
        Ok(event)
    }

    fn ensure_started(&self) -> Result<(), PeerStartupError> {
        self.identity.ensure_healthy()?;
        if !self.started {
            return Err(PeerStartupError::Dormant);
        }
        Ok(())
    }

    /// Close discovery tasks, listeners and transport connections.
    pub fn shutdown(&mut self) {
        if self.started {
            tracing::info!(target: "beacon::peers", "[beacon] Stopping checkpoint peer runtime");
        }
        self.peer.shutdown();
        self.started = false;
    }

    /// Retire one provider after startup admission, retaining ordinary terminal peer events.
    /// Retirement releases transport capacity and grants no consensus or availability receipt.
    pub fn retire_peer(&mut self, peer: PeerId) -> Result<(), PeerStartupError> {
        self.ensure_started()?;
        self.peer.retire_peer(peer);
        Ok(())
    }

    /// Advisory overlap from checked remote custody advertisements, never a sample receipt.
    pub(crate) fn custody_overlap(&self, peer: PeerId, columns: &[u64]) -> usize {
        if self.started {
            self.peer.custody_overlap(peer, columns)
        } else {
            0
        }
    }
}

impl Drop for CheckpointPeerRuntime {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn snapshot(
    config: &ChainConfig,
    checkpoint: &VerifiedCheckpoint,
    slot: Slot,
) -> Result<TrustedStatusSnapshot, PeerStartupError> {
    let finalized = checkpoint.checkpoint();
    if checkpoint.slot() > slot || finalized.epoch > slot / config.slots_per_epoch {
        return Err(PeerStartupError::ClockRegression);
    }
    Ok(TrustedStatusSnapshot {
        current_slot: slot,
        finalized,
        canonical_roots: vec![(finalized.epoch, finalized.root)],
        status: StatusV2 {
            status: Status {
                fork_digest: config.fork_digest(slot / config.slots_per_epoch)?,
                finalized_root: finalized.root,
                finalized_epoch: finalized.epoch,
                head_root: finalized.root,
                head_slot: checkpoint.slot(),
            },
            earliest_available_slot: checkpoint.slot(),
        },
    })
}

fn same_enr_content(left: &Enr, right: &Enr) -> bool {
    left.seq() == right.seq() && left.node_id() == right.node_id() && left.iter().eq(right.iter())
}

struct IdentityStore {
    path: PathBuf,
    identity: StoreIdentity,
    config: ChainConfig,
    digest: Root,
    generation: u64,
    key: CombinedKey,
    enr: Option<Enr>,
    signed_slot: Slot,
    policy: EndpointPolicy,
    poisoned: bool,
    _lock: File,
}

impl IdentityStore {
    fn open(
        repository_dir: &Path,
        identity: StoreIdentity,
        config: ChainConfig,
        digest: Root,
    ) -> Result<Self, PeerStartupError> {
        let path = repository_dir.join("peer-identity");
        fs::create_dir_all(&path)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700))
                .map_err(|error| fs::FsPathError::write(error, &path))?;
        }
        let lock_path = path.join("writer.lock");
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(|error| fs::FsPathError::open(error, &lock_path))?;
        lock.try_lock().map_err(|_| StoreError::AlreadyOpen(path.clone()))?;
        let mut newest = None;
        for (count, entry) in fs::read_dir(&path)?.enumerate() {
            if count >= MAX_DIRECTORY_ENTRIES {
                return Err(PeerStartupError::IdentityTooLarge);
            }
            let entry = entry.map_err(|error| fs::FsPathError::read_dir(error, &path))?;
            let name = entry.file_name();
            let name = name.to_str().ok_or(PeerStartupError::CorruptIdentity)?;
            if name == "writer.lock" {
                continue;
            }
            let generation = name
                .strip_prefix("identity-")
                .and_then(|name| name.strip_suffix(".commit"))
                .filter(|number| number.len() == 20)
                .and_then(|number| number.parse::<u64>().ok());
            if let Some(generation) = generation {
                newest = Some(newest.map_or(generation, |prior: u64| prior.max(generation)));
            } else if !name.starts_with("identity-") || !name.ends_with(".pending") {
                return Err(PeerStartupError::CorruptIdentity);
            }
        }
        let mut store = Self {
            path,
            identity,
            config,
            digest,
            generation: 0,
            key: CombinedKey::generate_secp256k1(),
            enr: None,
            signed_slot: 0,
            policy: EndpointPolicy::Public,
            poisoned: false,
            _lock: lock,
        };
        if let Some(generation) = newest {
            let path = store.snapshot_path(generation);
            let file = fs::open(&path)?;
            let length =
                file.metadata().map_err(|error| fs::FsPathError::metadata(error, &path))?.len();
            if length > MAX_IDENTITY_BYTES as u64 {
                return Err(PeerStartupError::IdentityTooLarge);
            }
            let mut bytes = Zeroizing::new(Vec::with_capacity(length as usize));
            file.take(MAX_IDENTITY_BYTES as u64 + 1)
                .read_to_end(&mut bytes)
                .map_err(|error| fs::FsPathError::read(error, &path))?;
            if bytes.len() > MAX_IDENTITY_BYTES {
                return Err(PeerStartupError::IdentityTooLarge);
            }
            store.decode(&bytes, generation)?;
        }
        Ok(store)
    }

    fn ensure_healthy(&self) -> Result<(), PeerStartupError> {
        if self.poisoned {
            return Err(StoreError::JournalPoisoned.into());
        }
        Ok(())
    }

    fn snapshot_path(&self, generation: u64) -> PathBuf {
        self.path.join(format!("identity-{generation:020}.commit"))
    }

    fn key_copy(&self) -> Result<CombinedKey, PeerStartupError> {
        self.ensure_healthy()?;
        let mut bytes = Zeroizing::new(self.key.encode());
        CombinedKey::secp256k1_from_bytes(&mut bytes).map_err(|_| PeerStartupError::CorruptIdentity)
    }

    fn decode(&mut self, bytes: &[u8], generation: u64) -> Result<(), PeerStartupError> {
        if bytes.len() < HEADER_BYTES + 32 ||
            &bytes[..8] != MAGIC ||
            bytes[8..12] != 1u32.to_le_bytes() ||
            bytes[12..20] != generation.to_le_bytes() ||
            generation == 0
        {
            return Err(PeerStartupError::CorruptIdentity);
        }
        let end = bytes.len() - 32;
        if sha256(&bytes[..end]) != bytes[end..] {
            return Err(PeerStartupError::CorruptIdentity);
        }
        if bytes[20..52] != self.identity.genesis_root ||
            bytes[52..84] != self.identity.config_digest ||
            bytes[84..116] != self.digest
        {
            return Err(StoreError::IdentityMismatch.into());
        }
        let slot = u64::from_le_bytes(
            bytes[116..124].try_into().map_err(|_| PeerStartupError::CorruptIdentity)?,
        );
        let policy = match bytes[124] {
            0 => EndpointPolicy::Public,
            1 => EndpointPolicy::LoopbackOnly,
            2 => EndpointPolicy::PrivateNetwork,
            _ => return Err(PeerStartupError::CorruptIdentity),
        };
        let mut secret = Zeroizing::new([0; 32]);
        secret.copy_from_slice(&bytes[125..157]);
        let key = CombinedKey::secp256k1_from_bytes(secret.as_mut())
            .map_err(|_| PeerStartupError::CorruptIdentity)?;
        let length = u16::from_le_bytes(
            bytes[157..159].try_into().map_err(|_| PeerStartupError::CorruptIdentity)?,
        ) as usize;
        if HEADER_BYTES.checked_add(length) != Some(end) {
            return Err(PeerStartupError::CorruptIdentity);
        }
        let text = std::str::from_utf8(&bytes[HEADER_BYTES..end])
            .map_err(|_| PeerStartupError::CorruptIdentity)?;
        let record = DiscoveryRecord::parse(text, &self.config, slot, policy)?;
        if record.compatibility != DiscoveryCompatibility::Matching ||
            record.enr().seq() == 0 ||
            record.enr().public_key() != key.public()
        {
            return Err(PeerStartupError::CorruptIdentity);
        }
        self.validate_record(&key, record.enr(), slot, policy)?;
        self.generation = generation;
        self.key = key;
        self.enr = Some(record.enr().clone());
        self.signed_slot = slot;
        self.policy = policy;
        Ok(())
    }

    fn validate_record(
        &self,
        key: &CombinedKey,
        enr: &Enr,
        slot: Slot,
        policy: EndpointPolicy,
    ) -> Result<(), PeerStartupError> {
        let (ip, udp, tcp, quic) = if let Some(ip) = enr.ip4() {
            (IpAddr::V4(ip), enr.udp4(), enr.tcp4(), enr.get_decodable::<u16>("quic"))
        } else {
            (
                IpAddr::V6(enr.ip6().ok_or(PeerStartupError::CorruptIdentity)?),
                enr.udp6(),
                enr.tcp6(),
                enr.get_decodable::<u16>("quic6"),
            )
        };
        // cgc is a canonical minimal big-endian integer in an RLP byte string. Vec<u8>
        // decodes an RLP list and rejects even this signer's valid zero/four advertisements.
        let custody = match enr.get_decodable::<u64>("cgc") {
            Some(Ok(count)) => count,
            Some(Err(_)) => return Err(PeerStartupError::CorruptIdentity),
            None => 0,
        };
        if !matches!(custody, 0 | 4) {
            return Err(PeerStartupError::CorruptIdentity)
        }
        let ad = LocalAdvertisement {
            discovery_socket: SocketAddr::new(ip, udp.ok_or(PeerStartupError::CorruptIdentity)?),
            tcp_port: tcp.ok_or(PeerStartupError::CorruptIdentity)?,
            quic_port: quic
                .ok_or(PeerStartupError::CorruptIdentity)?
                .map_err(|_| PeerStartupError::CorruptIdentity)?,
            attnets: [0; 8],
            syncnets: 0,
            custody_group_count: custody,
        };
        let expected = sign_local_enr(key, enr.seq(), &self.config, slot, &ad, policy)?;
        // The signer uses randomized ECDSA. Signed content, not a newly generated signature,
        // identifies an unchanged advertisement; the original signature is verified separately.
        if !enr.verify() || enr.public_key() != key.public() || !same_enr_content(&expected, enr) {
            return Err(PeerStartupError::CorruptIdentity);
        }
        Ok(())
    }

    fn persist(
        &mut self,
        record: &Enr,
        slot: Slot,
        policy: EndpointPolicy,
    ) -> Result<(), PeerStartupError> {
        self.ensure_healthy()?;
        if slot < self.signed_slot {
            return Err(PeerStartupError::ClockRegression);
        }
        if let Some(prior) = &self.enr {
            if record.seq() < prior.seq() ||
                (!same_enr_content(record, prior) && record.seq() <= prior.seq())
            {
                return Err(PeerStartupError::SequenceRegression);
            }
            if record == prior && slot == self.signed_slot && policy == self.policy {
                return Ok(());
            }
        }
        self.validate_record(&self.key, record, slot, policy)?;
        let generation = self.generation.checked_add(1).ok_or(StoreError::GenerationOverflow)?;
        let text = record.to_base64();
        let length = u16::try_from(text.len()).map_err(|_| PeerStartupError::IdentityTooLarge)?;
        let secret = Zeroizing::new(self.key.encode());
        if secret.len() != 32 || HEADER_BYTES + text.len() + 32 > MAX_IDENTITY_BYTES {
            return Err(PeerStartupError::IdentityTooLarge);
        }
        let mut bytes = Zeroizing::new(Vec::with_capacity(HEADER_BYTES + text.len() + 32));
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&1u32.to_le_bytes());
        bytes.extend_from_slice(&generation.to_le_bytes());
        bytes.extend_from_slice(&self.identity.genesis_root);
        bytes.extend_from_slice(&self.identity.config_digest);
        bytes.extend_from_slice(&self.digest);
        bytes.extend_from_slice(&slot.to_le_bytes());
        bytes.push(match policy {
            EndpointPolicy::Public => 0,
            EndpointPolicy::LoopbackOnly => 1,
            EndpointPolicy::PrivateNetwork => 2,
        });
        bytes.extend_from_slice(&secret);
        bytes.extend_from_slice(&length.to_le_bytes());
        bytes.extend_from_slice(text.as_bytes());
        let checksum = sha256(&bytes);
        bytes.extend_from_slice(&checksum);
        if let Err(error) = publish_file(&self.snapshot_path(generation), &bytes) {
            if matches!(error, StoreError::PublicationUncertain(_)) {
                self.poisoned = true;
            }
            return Err(error.into());
        }
        self.generation = generation;
        self.enr = Some(record.clone());
        self.signed_slot = slot;
        self.policy = policy;
        if generation > 2 {
            let _ = fs::remove_file_if_exists(self.snapshot_path(generation - 2));
        }
        Ok(())
    }
}

/// Scope experimental startup without changing any accepted consensus evidence.
fn validate_experimental_admission(
    config: &ChainConfig,
    listeners: PeerListenConfig,
    genesis_origin: Option<Root>,
) -> Result<(), PeerStartupError> {
    if config != &ChainConfig::hoodi() ||
        listeners.policy != EndpointPolicy::Public ||
        genesis_origin.is_some()
    {
        return Err(PeerStartupError::ExperimentalAdmission);
    }
    listeners.validate()?;
    Ok(())
}

#[cfg(all(
    feature = "peer-runtime",
    feature = "gossip-validation",
    feature = "execution-verification"
))]
mod import_status;

#[cfg(test)]
mod tests;

#[cfg(all(feature = "gossip-validation", feature = "execution-verification"))]
mod genesis;

#[cfg(all(feature = "gossip-validation", feature = "execution-verification"))]
mod custody;
