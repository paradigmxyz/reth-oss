//! One bounded event owner joining authenticated peer retrieval to strict signed import.
//! Only transport-checked compatible peers supply targets; their advertised heads remain
//! retrieval hints. Missing slots and envelopes never advance admitted chain authority.
//! The event loop continues servicing real availability replies while the dedicated importer
//! awaits them. One retained block/envelope batch leaves the second transport lease for DA.

use crate::{
    bootstrap::checked_config_digest,
    import_coordinator::{
        ImportCoordinator, ImportError, ImportReceipt, ServingBatch, ServingRequest,
    },
    peer_runtime::{CheckpointPeerRuntime, PeerStartupError},
};
use alloy_primitives::B256;
use libp2p::PeerId;
use reth_embedded_beacon_config::{ChainConfig, Preset as ConfigPreset};
use reth_embedded_beacon_network::{
    block_rpc::{BlockBatch, BlockRequest, WireBlock},
    discovery::DiscoveryError,
    envelope_rpc::{EnvelopeBatch, EnvelopeBlockReference, EnvelopeRequest, WireEnvelope},
    gossip::GossipKind,
    peer::{PeerRuntimeError, PeerRuntimeEvent},
    transport::{
        BlockRequestId, EnvelopeRequestId, GossipInput, GossipRequestId, GossipVerdict,
        SidecarRequestId, TransportEvent,
    },
    Status, StatusCompatibility,
};
use reth_embedded_beacon_primitives::{Fork, Root};
use reth_embedded_beacon_state_transition::ssz::{gloas, Preset};
use std::{
    collections::{HashSet, VecDeque},
    future::Future,
    pin::Pin,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::oneshot;

mod admission;
pub mod availability;
mod custody;
mod deadlines;
mod deferred;
mod envelope_broker;
mod envelope_retry;
mod history_broker;
mod observations;
mod ranges;
mod request_state;
mod retry;
mod serving;

use custody::CustodyTask;
use deferred::DeferredImport;
use envelope_broker::{BrokerEnvelopePending, RetryingBrokerEnvelope};
use envelope_retry::WaitingEnvelope;
use history_broker::HistoryBroker;
use request_state::RequestState;
use retry::{rotate_data_peer, DataPeerCycle, RetryingSidecar};
use serving::{ActiveServing, ServingToken};

pub use observations::{
    NetworkObservationSnapshot, PeerObservationHandle, PeerObservationSnapshot,
};

use availability::{
    CandidateGuard, EnvelopeRetrievalJob, RetrievalAvailability, RetrievalBroker, RetrievalError,
    RetrievalJob,
};

const MAX_PEERS: usize = 16;
const MAX_ANCESTRY_BLOCKS: usize = 2048;
const MAX_ANCESTRY_BYTES: usize = 32 * 1024 * 1024;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(25);
const DISCOVERY_INTERVAL: Duration = Duration::from_secs(30);

/// Gated peer event owner; this component cannot override compiled node readiness.
pub struct LiveSync {
    peer: Option<CheckpointPeerRuntime>,
    importer: ImportCoordinator,
    availability: RetrievalAvailability,
    broker: RetrievalBroker,
    config: ChainConfig,
    targets: Vec<Target>,
    rebase: bool,
    discovery_pending: bool,
    discovery_started: Instant,
    admitted_root: Root,
    admitted_slot: u64,
    cursor: u64,
    validated_range_cursor: u64,
    gossip_turn: bool,
    block_request: Option<BlockPending>,
    envelopes: RequestState<WaitingEnvelope, EnvelopePending>,
    sidecars: RequestState<RetryingSidecar, SidecarPending>,
    active: Option<ActiveImport>,
    stopped: oneshot::Receiver<()>,
    branch: Option<BranchRecovery>,
    observations: PeerObservationHandle,
    connected: HashSet<PeerId>,
    gossip_queue: VecDeque<QueuedGossip>,
    gossip_active: Option<ActiveGossip>,
    sidecar_cycle: DataPeerCycle,
    serving: Option<ActiveServing>,
    deferred_import: Option<DeferredImport>,
    broker_envelope: Option<BrokerEnvelopePending>,
    broker_envelope_retry: Option<RetryingBrokerEnvelope>,
    custody: CustodyTask,
    history: HistoryBroker,
}

impl std::fmt::Debug for LiveSync {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LiveSync")
            .field("network", &self.config.name)
            .field("peers", &self.targets.len())
            .field("admitted_slot", &self.admitted_slot)
            .field("cursor", &self.cursor)
            .field("active_import", &self.active.is_some())
            .finish_non_exhaustive()
    }
}
impl LiveSync {
    /// Bind independently created components to the importer's concrete configured graph.
    /// The actual snapshot is queried from its worker, never supplied by a caller.
    pub async fn new(
        peer: CheckpointPeerRuntime,
        importer: ImportCoordinator,
        availability: RetrievalAvailability,
        broker: RetrievalBroker,
        config: ChainConfig,
    ) -> Result<(Self, LiveSyncCancellation), LiveSyncError> {
        let digest = checked_config_digest(&config).map_err(|_| LiveSyncError::Configuration)?;
        if importer.config_digest() != digest ||
            peer.config_digest()? != digest ||
            availability.config_digest() != digest ||
            broker.digest != digest
        {
            return Err(LiveSyncError::Configuration);
        }
        availability.bind_peer_identity(peer.local_enr()?.node_id().raw())?;
        importer.set_column_retention(std::sync::Arc::new(availability.clone())).await?;
        let snapshot = importer.snapshot().await?;
        let (stop, stopped) = oneshot::channel();
        let root = snapshot.head.root;
        let slot = snapshot.head_slot;
        Ok((
            Self {
                peer: Some(peer),
                importer,
                availability,
                broker,
                config,
                targets: Vec::new(),
                rebase: false,
                discovery_pending: false,
                discovery_started: Instant::now(),
                admitted_root: root,
                admitted_slot: slot,
                cursor: slot.checked_add(1).ok_or(LiveSyncError::Bounds)?,
                validated_range_cursor: slot.checked_add(1).ok_or(LiveSyncError::Bounds)?,
                gossip_turn: true,
                block_request: None,
                envelopes: Default::default(),
                sidecars: Default::default(),
                active: None,
                stopped,
                branch: None,
                observations: PeerObservationHandle::default(),
                connected: HashSet::new(),
                gossip_queue: VecDeque::new(),
                gossip_active: None,

                sidecar_cycle: DataPeerCycle::default(),
                serving: None,
                deferred_import: None,

                broker_envelope: None,
                broker_envelope_retry: None,
                custody: CustodyTask::default(),
                history: HistoryBroker::default(),
            },
            LiveSyncCancellation { stop: Some(stop) },
        ))
    }

    /// Clone the same bounded observation handle without granting graph mutation authority.
    pub fn observation_handle(&self) -> ImportCoordinator {
        self.importer.clone()
    }

    /// Read network scheduling counters from this exact event owner.
    pub fn peer_observation_handle(&self) -> PeerObservationHandle {
        self.observations.clone()
    }

    fn observe(&self) {
        self.observations.retrieval_cursor(self.cursor);
        self.observations.work(
            self.gossip_queue.len(),
            self.gossip_active.is_some(),
            self.availability.observation(),
        );
        self.observations.connections(
            self.connected.len(),
            self.targets.iter().map(|target| target.status.head_slot).max().unwrap_or(0),
            true,
        );
        self.observations.update(
            self.targets.len(),
            usize::from(self.block_request.is_some()) +
                usize::from(self.envelopes.requested().is_some()) +
                usize::from(self.sidecars.requested().is_some()) +
                usize::from(self.sidecars.waiting().is_some()) +
                usize::from(self.envelopes.waiting().is_some()) +
                usize::from(self.broker_envelope.is_some()) +
                usize::from(self.broker_envelope_retry.is_some()) +
                self.history.requests(),
            self.active.is_some() || self.deferred_import.is_some(),
            self.discovery_pending,
            self.branch.as_ref().map_or(0, |branch| branch.blocks.len()),
            self.branch.as_ref().map_or(0, |branch| branch.bytes),
        );
    }

    /// Open only through the existing compiled readiness gate, then drive one actual owner.
    /// An uncertain import or local broker failure closes IO and requires reconciliation.
    pub async fn run(mut self) -> Result<(), LiveSyncError> {
        self.publish_status().await?;
        self.peer.as_mut().ok_or(LiveSyncError::PeerUnavailable)?.start().await?;
        self.discover()?;
        let result = self.drive().await;
        if let Some(peer) = &mut self.peer {
            peer.shutdown();
        }
        self.connected.clear();
        self.observations.connections(0, 0, false);
        result
    }

    async fn drive(&mut self) -> Result<(), LiveSyncError> {
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        loop {
            if self.importer.is_halted() || self.availability.is_halted() {
                return Err(LiveSyncError::ImporterHalted);
            }
            if self.rebase && self.active.is_none() && self.deferred_import.is_none() {
                if let Some(actual) = admission::observation(self.importer.snapshot().await)? {
                    self.admitted_root = actual.head.root;
                    self.admitted_slot = actual.head_slot;
                    self.rebase_cursor(actual.head_slot)?;
                    self.rebase = false;
                }
            }
            self.schedule_sidecar_retry()?;
            self.retry_deferred_import().await?;
            self.schedule_envelope_retry()?;
            self.schedule_broker_envelope()?;
            self.schedule_history()?;
            self.schedule().await?;
            self.observe();
            let has_import = self.active.is_some();
            let has_gossip = self.gossip_active.is_some();
            let has_serving = self.serving.is_some();
            let event = tokio::select! {
                result = self.peer.as_mut().ok_or(LiveSyncError::PeerUnavailable)?.next_event() => DriverEvent::Peer(result?),
                job = self.broker.receiver.recv() => DriverEvent::Retrieval(job.ok_or(LiveSyncError::BrokerClosed)?),
                job = self.broker.envelopes.recv() => DriverEvent::EnvelopeRetrieval(job.ok_or(LiveSyncError::BrokerClosed)?),
                job = self.broker.history.recv() => DriverEvent::HistoricalRetrieval(job.ok_or(LiveSyncError::BrokerClosed)?),
                result = async {
                    self.active.as_mut().expect("select guarded active import").reply.as_mut().await
                }, if has_import => DriverEvent::Imported(result),
                result = async { self.gossip_active.as_mut().expect("guarded native gossip future").reply.as_mut().await }, if has_gossip => DriverEvent::Gossip(result),
                result = async { self.serving.as_mut().expect("guarded bounded serving future").reply.as_mut().await }, if has_serving => DriverEvent::Served(result),
                _ = tick.tick() => DriverEvent::Tick,
                _ = &mut self.stopped => return Ok(()),
            };
            match event {
                DriverEvent::Peer(PeerRuntimeEvent::Transport(event)) => {
                    self.transport(event).await?
                }
                DriverEvent::Peer(PeerRuntimeEvent::DiscoveryComplete { candidate_count }) => {
                    self.observations.discovery(candidate_count);
                    self.discovery_pending = false;
                }
                DriverEvent::Peer(PeerRuntimeEvent::CandidateRejected { .. }) => {
                    self.observations.candidate_rejected()
                }
                DriverEvent::Peer(PeerRuntimeEvent::Tick) => {}
                DriverEvent::Retrieval(job) => self.retrieve(job)?,
                DriverEvent::EnvelopeRetrieval(job) => self.retrieve_envelope(job)?,
                DriverEvent::HistoricalRetrieval(job) => self.history_request(job)?,
                DriverEvent::Imported(result) => {
                    self.imported(result)?;
                    self.publish_status().await?;
                }
                DriverEvent::Gossip(result) => {
                    self.gossip_validated(result)?;
                    self.publish_status().await?;
                }
                DriverEvent::Served(result) => self.served(result)?,
                DriverEvent::Tick => {
                    self.expire_serving()?;
                    self.expire()?;
                    if !self.discovery_pending &&
                        self.discovery_started.elapsed() >= DISCOVERY_INTERVAL
                    {
                        self.discover()?;
                    }
                    if self.active.is_none() && self.gossip_active.is_none() {
                        self.publish_status().await?;
                    }
                }
            }
        }
    }

    fn discover(&mut self) -> Result<(), LiveSyncError> {
        if !self.discovery_pending {
            self.discovery_pending = discovery_scheduled(
                self.peer.as_mut().ok_or(LiveSyncError::PeerUnavailable)?.request_discovery(),
            )?;
            self.discovery_started = Instant::now();
        }
        Ok(())
    }
    async fn publish_status(&mut self) -> Result<(), LiveSyncError> {
        let Some(mut status) = admission::observation(self.importer.authenticated_status().await)?
        else {
            return Ok(())
        };
        let receipt = self
            .custody_receipt(
                status.snapshot().head.root,
                status.current_slot(),
                status.snapshot().head_slot,
            )
            .await?;
        status.apply_custody_history(receipt)?;
        let mut peer = self.peer.take().ok_or(LiveSyncError::PeerUnavailable)?;
        let (restored, result) = tokio::task::spawn_blocking(move || {
            let result = peer
                .apply_authenticated_status(status)
                .inspect_err(|error| {
                    tracing::error!(target: "beacon::peers", %error,
                        "[beacon] Failed to publish authenticated peer status");
                })
                .and_then(|()| {
                    peer.apply_custody(receipt).inspect_err(|error| {
                        tracing::error!(target: "beacon::peers", %error,
                            "[beacon] Failed to publish verified custody advertisement");
                    })
                });
            (peer, result)
        })
        .await
        .map_err(|_| LiveSyncError::PeerUnavailable)?;
        self.peer = Some(restored);
        result?;
        Ok(())
    }
    async fn schedule(&mut self) -> Result<(), LiveSyncError> {
        if self.rebase ||
            self.history.requests() != 0 ||
            self.availability.custody_retrieving() ||
            self.active.is_some() ||
            self.deferred_import.is_some() ||
            self.block_request.is_some() ||
            !self.envelopes.is_idle() ||
            !self.sidecars.is_idle() ||
            self.gossip_active.is_some()
        {
            return Ok(());
        }
        if self.gossip_turn &&
            let Some(queued) = self.gossip_queue.pop_front()
        {
            self.gossip_turn = false;
            self.start_gossip(queued).await?;
            return Ok(());
        }
        if let Some(branch) = self.branch.as_mut() {
            if let Some(root) = branch.next_root {
                let Some(retained) = admission::contains_block(&self.importer, root).await? else {
                    return Ok(())
                };
                if retained {
                    branch.next_root = None;
                } else {
                    let peer = branch.peer;
                    let Some(id) =
                        self.try_request_blocks(peer, BlockRequest::Roots(vec![root]))?
                    else {
                        return Ok(())
                    };
                    self.block_request = Some(BlockPending {
                        peer,
                        id,
                        request: BlockRequest::Roots(vec![root]),
                        started: Instant::now(),
                    });
                    self.gossip_turn = true;
                    return Ok(());
                }
            }
            if let Some(wire) = branch.blocks.front() {
                let Some(guard) = admission::candidate(self.availability.bind_candidate(wire))?
                else {
                    return Ok(())
                };
                let wire = branch.blocks.pop_front().ok_or(LiveSyncError::Response)?;
                branch.bytes =
                    branch.bytes.checked_sub(wire.ssz().len()).ok_or(LiveSyncError::Bounds)?;
                let peer = branch.peer;
                let metadata = EnvelopeBlockReference::from(&wire);
                let is_gloas = wire.fork() == Fork::Gloas;
                let root = wire.root();
                let slot = wire.slot();
                let range_next_slot = branch.range_next_slot_for(&wire)?;
                let reply = Box::pin(self.importer.import_block(wire.clone(), now()?));
                self.active = Some(ActiveImport {
                    peer,
                    root,
                    slot,
                    kind: ImportKind::Block { is_gloas, metadata },
                    reply,
                    _batch: RetainedBatch::Branch { _wire: wire },
                    _candidate: guard,
                    gossip: None,
                    range_next_slot,
                });
                self.gossip_turn = true;
                return Ok(());
            }
            let completed = self.branch.take().ok_or(LiveSyncError::Response)?;
            if let Some(target) =
                self.targets.iter_mut().find(|target| target.peer == completed.peer)
            {
                target.examined_root = Some(completed.target_root);
            }
        }
        if let Some(target) = self.range_target() {
            let peer = target.peer;
            let request = ranges::catch_up_request(self.cursor, target.status.head_slot)?;
            let Some(id) = self.try_request_blocks(peer, request.clone())? else { return Ok(()) };
            self.block_request = Some(BlockPending { peer, id, request, started: Instant::now() });
            self.gossip_turn = true;
            return Ok(());
        }
        if let Some(target) = self.targets.iter().find(|target| {
            target.status.head_root != self.admitted_root &&
                target.examined_root != Some(target.status.head_root)
        }) {
            let peer = target.peer;
            let root = target.status.head_root;
            let Some(retained) = admission::contains_block(&self.importer, root).await? else {
                return Ok(())
            };
            if retained {
                if let Some(target) = self.targets.iter_mut().find(|target| target.peer == peer) {
                    target.examined_root = Some(root);
                }
                return Ok(());
            }
            self.branch = Some(BranchRecovery {
                peer,
                target_root: root,
                next_root: Some(root),
                blocks: VecDeque::new(),
                bytes: 0,
                range_next_slot: None,
                range_start_slot: None,
            });
        }
        // Without eligible catch-up work, gossip still runs on every idle scheduling turn.
        if let Some(queued) = self.gossip_queue.pop_front() {
            self.gossip_turn = false;
            self.start_gossip(queued).await?;
        }
        Ok(())
    }

    async fn transport(&mut self, event: TransportEvent) -> Result<(), LiveSyncError> {
        self.observations.transport(&event);
        match event {
            TransportEvent::PeerStatus {
                peer,
                status,
                earliest_available_slot,
                compatibility:
                    StatusCompatibility::KnownFinality | StatusCompatibility::NeedsFinalizedRoot { .. },
            } => {
                self.connected.insert(peer);
                if let Some(target) = self.targets.iter_mut().find(|target| target.peer == peer) {
                    if target.status.head_root != status.head_root {
                        target.examined_root = None;
                    }
                    target.status = status;
                    target.earliest = earliest_available_slot;
                } else if self.targets.len() < MAX_PEERS {
                    self.targets.push(Target {
                        peer,
                        status,
                        earliest: earliest_available_slot,
                        examined_root: None,
                    });
                }
            }
            TransportEvent::Closed(peer) | TransportEvent::Rejected { peer, .. } => {
                self.connected.remove(&peer);
                if self.serving.as_ref().is_some_and(|pending| pending.peer == peer) {
                    self.serving = None;
                }
                // These events follow resolution of all channels belonging to the disconnected
                // peer. Running codec and import jobs retain their own permits until completion.
                self.remove_peer(peer);
                self.history_disconnected(peer)?;
                let rebase =
                    if self.block_request.as_ref().is_some_and(|pending| pending.peer == peer) {
                        self.block_request = None;
                        true
                    } else {
                        false
                    };
                if self.envelopes.requested().is_some_and(|pending| pending.peer == peer) {
                    let pending = self.envelopes.take_requested().ok_or(LiveSyncError::Response)?;
                    self.retry_envelope(pending)?;
                }
                if self.broker_envelope.as_ref().is_some_and(|pending| pending.peer == peer) {
                    let pending = self.broker_envelope.take().ok_or(LiveSyncError::Response)?;
                    self.retry_broker_envelope(pending)?;
                }
                if self.sidecars.requested().is_some_and(|pending| pending.peer == peer) {
                    let pending = self.sidecars.take_requested().ok_or(LiveSyncError::Response)?;
                    self.retry_sidecar(pending)?;
                }
                if rebase {
                    self.reset_cursor()?;
                }
            }
            TransportEvent::Blocks { peer, id, batch } => {
                if self.history.matches(peer, id) {
                    return self.history_received(peer, id, batch);
                }
                if !self
                    .block_request
                    .as_ref()
                    .is_some_and(|pending| pending.peer == peer && pending.id == id)
                {
                    // A closed or replaced request can leave a completed transport event. Drop
                    // its bounded response lease without taking the current request's owner.
                    tracing::debug!(target: "beacon::sync", %peer, ?id,
                        "[beacon] Discarded response without its current request association");
                    return Ok(())
                }
                let pending = self.block_request.take().ok_or(LiveSyncError::Response)?;
                pending.request.matches(batch.blocks()).map_err(|_| LiveSyncError::Response)?;
                if let BlockRequest::Range { start_slot, count } = pending.request {
                    if self.branch.is_some() {
                        return Err(LiveSyncError::Bounds);
                    }
                    if batch.blocks().is_empty() {
                        // An empty matched response changes only the retrieval hint. A later
                        // child still requires exact native ancestry and original state checks.
                        self.cursor = start_slot.checked_add(count).ok_or(LiveSyncError::Bounds)?;
                    } else {
                        self.branch = Some(BranchRecovery::from_range(peer, batch.blocks())?);
                    }
                    // The independently bounded immutable originals keep their own byte
                    // budget. Release the bulk transport lease before native import or DA.
                    drop(batch);
                    return self.schedule().await;
                }
                if batch.blocks().is_empty() {
                    self.branch = None;
                    self.remove_peer(peer);
                    return Ok(());
                }
                let wire = batch.blocks()[0].clone();
                let range_next_slot = None;
                let needs_parent_recovery = if self.branch.is_some() {
                    true
                } else {
                    let Some(retained) =
                        admission::contains_block(&self.importer, wire.parent_root()).await?
                    else {
                        // Release the response lease; the unchanged cursor retries retrieval.
                        return Ok(())
                    };
                    !retained
                };
                if needs_parent_recovery {
                    self.recover_parent(peer, wire, range_next_slot)?;
                    // Only original immutable bytes remain under the independent ancestry budget;
                    // release this bulk reservation before requesting another exact parent.
                    drop(batch);
                    return Ok(());
                }
                let Some(retained) = admission::contains_block(&self.importer, wire.root()).await?
                else {
                    return Ok(())
                };
                if retained {
                    if let Some(next_slot) = range_next_slot {
                        self.advance_range_cursor(next_slot);
                    }
                    return Ok(());
                }
                let wire_slot = wire.slot();
                let metadata = EnvelopeBlockReference::from(&wire);
                let is_gloas = wire.fork() == Fork::Gloas;
                let Some(guard) = admission::candidate(self.availability.bind_candidate(&wire))?
                else {
                    // The completed response releases its lease; the unchanged cursor retries.
                    return Ok(())
                };
                let reply = Box::pin(self.importer.import_block(wire, now()?));
                self.active = Some(ActiveImport {
                    peer,
                    root: batch.blocks()[0].root(),
                    slot: wire_slot,
                    kind: ImportKind::Block { is_gloas, metadata },
                    reply,
                    _batch: RetainedBatch::Blocks { _batch: batch },
                    _candidate: guard,
                    gossip: None,
                    range_next_slot,
                });
            }
            TransportEvent::BlocksUnavailable { peer, id, .. } => {
                if self.history.matches(peer, id) {
                    return self.history_unavailable(peer, id);
                }
                if !self
                    .block_request
                    .as_ref()
                    .is_some_and(|pending| pending.peer == peer && pending.id == id)
                {
                    // A closed or replaced request can leave a completed transport event. Drop
                    // its bounded response lease without taking the current request's owner.
                    tracing::debug!(target: "beacon::sync", %peer, ?id,
                        "[beacon] Discarded response without its current request association");
                    return Ok(())
                }
                self.block_request.take().ok_or(LiveSyncError::Response)?;
                self.remove_peer(peer);
                self.reset_cursor()?;
            }
            TransportEvent::Envelopes { peer, id, batch } => {
                if self
                    .broker_envelope
                    .as_ref()
                    .is_some_and(|pending| pending.peer == peer && pending.id == id)
                {
                    return self.broker_envelope_result(peer, id, batch);
                }
                if !self
                    .envelopes
                    .requested()
                    .is_some_and(|pending| pending.peer == peer && pending.id == id)
                {
                    // A closed or replaced request can leave a completed transport event. Drop
                    // its bounded response lease without taking the current request's owner.
                    tracing::debug!(target: "beacon::sync", %peer, ?id,
                        "[beacon] Discarded response without its current request association");
                    return Ok(())
                }
                let pending = self.envelopes.take_requested().ok_or(LiveSyncError::Response)?;
                EnvelopeRequest::Roots(vec![pending.root])
                    .matches(&self.config, &batch, |root| {
                        (root == pending.root).then_some(&pending.metadata)
                    })
                    .map_err(|_| LiveSyncError::Response)?;
                if batch.envelopes().is_empty() {
                    self.retry_envelope(pending)?;
                    return Ok(());
                }
                if batch.envelopes().len() != 1 {
                    return Err(LiveSyncError::Response);
                }
                let envelope = batch.envelopes()[0].clone();
                let guard = pending.candidate;
                let reply = Box::pin(self.importer.import_envelope(envelope, now()?));
                self.active = Some(ActiveImport {
                    peer,
                    root: pending.root,
                    slot: pending.slot,
                    kind: ImportKind::Envelope,
                    reply,
                    _batch: RetainedBatch::Envelopes { _batch: batch },
                    _candidate: guard,
                    gossip: None,
                    range_next_slot: None,
                });
            }
            TransportEvent::EnvelopesUnavailable { peer, id, code } => {
                if self
                    .broker_envelope
                    .as_ref()
                    .is_some_and(|pending| pending.peer == peer && pending.id == id)
                {
                    let pending = self.broker_envelope.take().ok_or(LiveSyncError::Response)?;
                    self.retire_unavailable_provider(peer, code)?;
                    return self.retry_broker_envelope(pending);
                }
                if !self
                    .envelopes
                    .requested()
                    .is_some_and(|pending| pending.peer == peer && pending.id == id)
                {
                    // A closed or replaced request can leave a completed transport event. Drop
                    // its bounded response lease without taking the current request's owner.
                    tracing::debug!(target: "beacon::sync", %peer, ?id,
                        "[beacon] Discarded response without its current request association");
                    return Ok(())
                }
                let pending = self.envelopes.take_requested().ok_or(LiveSyncError::Response)?;
                self.retire_unavailable_provider(peer, code)?;
                // Keep the admitted beacon graph; this response proves no execution branch.
                self.retry_envelope(pending)?;
            }
            TransportEvent::Sidecars { peer, id, batch } => {
                if !self
                    .sidecars
                    .requested()
                    .is_some_and(|pending| pending.peer == peer && pending.id == id)
                {
                    // A closed or replaced request can leave a completed transport event. Drop
                    // its bounded response lease without taking the current request's owner.
                    tracing::debug!(target: "beacon::sync", %peer, ?id,
                        "[beacon] Discarded response without its current request association");
                    return Ok(())
                }
                let pending = self.sidecars.take_requested().ok_or(LiveSyncError::Response)?;
                let result = pending
                    .job
                    .request
                    .matches(&self.config, &batch)
                    .map(|()| batch)
                    .map_err(|_| RetrievalError::Association);
                pending.job.complete(result);
            }
            TransportEvent::SidecarsUnavailable { peer, id, code } => {
                if !self
                    .sidecars
                    .requested()
                    .is_some_and(|pending| pending.peer == peer && pending.id == id)
                {
                    // A closed or replaced request can leave a completed transport event. Drop
                    // its bounded response lease without taking the current request's owner.
                    tracing::debug!(target: "beacon::sync", %peer, ?id,
                        "[beacon] Discarded response without its current request association");
                    return Ok(())
                }
                let pending = self.sidecars.take_requested().ok_or(LiveSyncError::Response)?;
                self.remove_peer(peer);
                self.retire_unavailable_provider(peer, code)?;
                self.retry_sidecar(pending)?;
            }
            TransportEvent::BlockRequest { peer, id, request } => {
                self.serve_request(
                    peer,
                    ServingToken::Blocks(id),
                    ServingRequest::Blocks(request),
                )?;
            }
            TransportEvent::EnvelopeRequest { peer, id, request } => {
                self.serve_request(
                    peer,
                    ServingToken::Envelopes(id),
                    ServingRequest::Envelopes(request),
                )?;
            }
            TransportEvent::SidecarRequest { peer, id, request } => {
                self.serve_sidecars_request(peer, id, request)?;
            }
            // All peer head/finality advertisements remain retrieval hints until signed admission.
            TransportEvent::Listening(_) |
            TransportEvent::DialFailed(_) |
            TransportEvent::PeerPing { .. } |
            TransportEvent::PeerMetadata { .. } |
            TransportEvent::ControlUnavailable { .. } => {}
            TransportEvent::Gossip { peer, id, input } => {
                self.observations.received();
                if self.gossip_queue.len() < 8 {
                    self.gossip_queue.push_back(QueuedGossip { peer, id, input });
                } else {
                    self.report_gossip(id, GossipVerdict::Ignore);
                }
            }
        }
        Ok(())
    }

    fn retrieve(&mut self, job: RetrievalJob) -> Result<(), LiveSyncError> {
        if job.reply.is_closed() {
            job.complete(Err(RetrievalError::Missing));
            return Ok(())
        }
        let owned = self
            .active
            .as_ref()
            .is_some_and(|active| active.root == job.root && active.slot == job.slot) ||
            (self.active.is_none() && self.availability.custody_retrieving());
        if !owned || !self.availability.owns_retrieval_job(&job)? {
            // Custody and imports share the retained candidate seal. An unrelated job grants
            // no transport request or availability, and cannot replace another queued owner.
            job.complete(Err(RetrievalError::Association));
            tracing::debug!(target: "beacon::sync",
                "[beacon] Discarded availability job without its retained original candidate");
            return Ok(())
        }
        if !self.sidecars.is_idle() {
            job.complete(Err(RetrievalError::Capacity));
            return Ok(())
        }
        let started = job.created;
        self.sidecars.wait(RetryingSidecar { job, started, tried: Vec::new() })?;
        self.schedule_sidecar_retry()?;
        Ok(())
    }
    fn imported(
        &mut self,
        result: Result<ImportReceipt, ImportError>,
    ) -> Result<(), LiveSyncError> {
        let active = self.active.take().ok_or(LiveSyncError::Response)?;
        if !self.sidecars.is_idle() ||
            self.broker_envelope.is_some() ||
            self.broker_envelope_retry.is_some()
        {
            return Err(LiveSyncError::Response);
        }
        if self.availability.is_halted() {
            return Err(LiveSyncError::BrokerClosed);
        }
        let receipt = match result {
            Ok(receipt) if receipt.admitted_root == active.root => {
                if let Some(id) = active.gossip {
                    self.report_gossip(id, GossipVerdict::Accept);
                }
                receipt
            }
            Ok(_) => {
                if let Some(id) = active.gossip {
                    self.report_gossip(id, GossipVerdict::Ignore);
                }
                return Err(LiveSyncError::Response);
            }
            Err(ImportError::Backpressure) if !self.importer.is_halted() => {
                tracing::debug!(target: "beacon::sync", root = %B256::from(active.root),
                    "[beacon] Deferring original input until bounded import admission is available");
                self.defer_import(active)?;
                return Ok(());
            }
            Err(ImportError::ExecutionPending(root))
                if root == active.root && !self.importer.is_halted() =>
            {
                self.defer_import(active)?;
                return Ok(());
            }
            Err(error) if self.availability.deferred_data() && !self.importer.is_halted() => {
                if let Some(id) = active.gossip {
                    self.report_gossip(id, GossipVerdict::Ignore);
                }
                tracing::debug!(target: "beacon::sync", %error, "[beacon] Required original data remains unavailable");
                self.remove_peer(active.peer);
                self.reset_cursor()?;
                return Ok(());
            }
            Err(ImportError::GossipIgnore(reason)) => {
                if let Some(id) = active.gossip {
                    self.report_gossip(id, GossipVerdict::Ignore);
                }
                tracing::debug!(target: "beacon::gossip", reason, "[beacon] Deferred original gossip input");
                return Ok(());
            }
            Err(
                error @ (ImportError::GossipReject(_) |
                ImportError::Consensus(_) |
                ImportError::Envelope(_) |
                ImportError::Wire),
            ) if !self.importer.is_halted() => {
                if let Some(id) = active.gossip {
                    self.report_gossip(id, GossipVerdict::Reject);
                }
                tracing::debug!(target: "beacon::sync", peer = %active.peer, %error, "[beacon] Rejected retrieved original consensus input");
                self.remove_peer(active.peer);
                self.reset_cursor()?;
                return Ok(());
            }
            Err(error) => {
                if let Some(id) = active.gossip {
                    self.report_gossip(id, GossipVerdict::Ignore);
                }
                return Err(error.into());
            }
        };
        tracing::info!(target: "beacon::sync", slot = active.slot,
            admitted_root = %B256::from(active.root),
            head_root = %B256::from(receipt.snapshot.head.root),
            head_payload_status = ?receipt.snapshot.head.payload_status,
            head_slot = receipt.snapshot.head_slot,
            finalized_epoch = receipt.snapshot.finalized_checkpoint.epoch,
            "[beacon] Advanced actual admitted peer sync graph");
        if let ImportKind::Block { is_gloas, metadata } = active.kind {
            self.admitted_root = receipt.snapshot.head.root;
            self.admitted_slot = receipt.snapshot.head_slot;
            // Canonical selection can lag a strictly admitted range block during catch-up.
            // Gossip and unrelated ancestry imports do not consume the forward range.
            if let Some(next_slot) = active.range_next_slot {
                self.advance_range_cursor(next_slot);
            }
            let peer = active.peer;
            let root = active.root;
            let slot = active.slot;
            // Release the original block reservation before requesting an envelope. Keep only
            // the small exact metadata and candidate-signature context for subsequent DA.
            drop(active._batch);
            if is_gloas {
                if !self.envelopes.is_idle() {
                    return Err(LiveSyncError::Bounds);
                }
                self.envelopes.wait(WaitingEnvelope {
                    root,
                    slot,
                    metadata,
                    candidate: active._candidate,
                    started: Instant::now(),
                    tried: Vec::new(),
                    preferred: peer,
                })?;
                self.schedule_envelope_retry()?;
                self.schedule_broker_envelope()?;
                self.schedule_history()?;
            }
        }
        Ok(())
    }

    async fn start_gossip(&mut self, queued: QueuedGossip) -> Result<(), LiveSyncError> {
        if matches!(queued.input.topic().kind, GossipKind::Envelope) {
            let preset = match self.config.preset {
                ConfigPreset::Mainnet => Preset::Mainnet,
                ConfigPreset::Minimal => Preset::Minimal,
            };
            let signed =
                match gloas::SignedExecutionPayloadEnvelope::from_ssz(queued.input.ssz(), preset) {
                    Ok(signed) => signed,
                    Err(_) => {
                        self.report_gossip(queued.id, GossipVerdict::Reject);
                        return Ok(());
                    }
                };
            let root = signed.message.beacon_block_root;
            let Some(reference) =
                admission::observation(self.importer.block_reference(root).await)?.flatten()
            else {
                self.report_gossip(queued.id, GossipVerdict::Ignore);
                return Ok(());
            };
            if reference.context() != queued.input.topic().digest {
                self.report_gossip(queued.id, GossipVerdict::Reject);
                return Ok(());
            }
            let wire = match WireEnvelope::from_reference(
                queued.input.ssz().to_vec(),
                &reference,
                &self.config,
            ) {
                Ok(wire) => wire,
                Err(_) => {
                    self.report_gossip(queued.id, GossipVerdict::Reject);
                    return Ok(());
                }
            };
            let Some(guard) =
                admission::candidate(self.availability.bind_envelope_reference(&reference))?
            else {
                self.report_gossip(queued.id, GossipVerdict::Ignore);
                return Ok(())
            };
            let reply = Box::pin(self.importer.import_gossip_envelope(wire, now()?));
            self.active = Some(ActiveImport {
                peer: queued.peer,
                root,
                slot: reference.slot(),
                kind: ImportKind::Envelope,
                reply,
                _batch: RetainedBatch::Gossip { _input: queued.input },
                _candidate: guard,
                gossip: Some(queued.id),
                range_next_slot: None,
            });
            return Ok(());
        }
        if matches!(queued.input.topic().kind, GossipKind::Block) {
            let wire = match WireBlock::from_ssz(queued.input.ssz().to_vec(), &self.config) {
                Ok(wire)
                    if wire.fork() == queued.input.topic().fork &&
                        wire.context() == queued.input.topic().digest =>
                {
                    wire
                }
                _ => {
                    self.report_gossip(queued.id, GossipVerdict::Reject);
                    return Ok(());
                }
            };
            let Some(parent_retained) =
                admission::contains_block(&self.importer, wire.parent_root()).await?
            else {
                self.report_gossip(queued.id, GossipVerdict::Ignore);
                return Ok(())
            };
            if !parent_retained {
                self.report_gossip(queued.id, GossipVerdict::Ignore);
                // Current-head gossip must not preempt sequential catch-up with a distant
                // ancestry walk. Matched range inputs recover their own missing parents.
                if self.branch.is_none() && self.range_target().is_none() {
                    self.recover_parent(queued.peer, wire, None)?;
                }
                return Ok(());
            }
            let metadata = EnvelopeBlockReference::from(&wire);
            let is_gloas = wire.fork() == Fork::Gloas;
            let root = wire.root();
            let slot = wire.slot();
            let Some(guard) = admission::candidate(self.availability.bind_candidate(&wire))? else {
                self.report_gossip(queued.id, GossipVerdict::Ignore);
                return Ok(())
            };
            let reply = Box::pin(self.importer.import_gossip_block(wire, now()?));
            self.active = Some(ActiveImport {
                peer: queued.peer,
                root,
                slot,
                kind: ImportKind::Block { is_gloas, metadata },
                reply,
                _batch: RetainedBatch::Gossip { _input: queued.input },
                _candidate: guard,
                gossip: Some(queued.id),
                range_next_slot: None,
            });
            return Ok(());
        }
        let reply = Box::pin(self.importer.gossip(
            queued.input.topic(),
            std::sync::Arc::from(queued.input.ssz()),
            now()?,
        ));
        self.gossip_active = Some(ActiveGossip { id: queued.id, reply, _input: queued.input });
        Ok(())
    }
    fn gossip_validated(
        &mut self,
        result: Result<GossipVerdict, ImportError>,
    ) -> Result<(), LiveSyncError> {
        let active = self.gossip_active.take().ok_or(LiveSyncError::Response)?;
        match result {
            Ok(verdict) => self.report_gossip(active.id, verdict),
            Err(error) => {
                self.report_gossip(active.id, GossipVerdict::Ignore);
                if !matches!(
                    error,
                    ImportError::Bounds |
                        ImportError::Backpressure |
                        ImportError::GossipIgnore(_) |
                        ImportError::GossipReject(_)
                ) {
                    return Err(error.into());
                }
            }
        }
        Ok(())
    }
    fn report_gossip(&mut self, id: GossipRequestId, verdict: GossipVerdict) {
        self.observations.verdict(verdict);
        if let Some(peer) = &mut self.peer {
            // Disconnect or finite wire-cache expiry can retire the token while native work
            // continues. Its committed graph receipt remains independent of propagation.
            if let Err(error) = peer.report_gossip(id, verdict) {
                tracing::debug!(target: "beacon::gossip", %error, "[beacon] Gossip propagation callback already retired");
            }
        }
    }

    fn recover_parent(
        &mut self,
        peer: PeerId,
        wire: WireBlock,
        range_next_slot: Option<u64>,
    ) -> Result<(), LiveSyncError> {
        let branch = self.branch.get_or_insert_with(|| BranchRecovery {
            peer,
            target_root: wire.root(),
            next_root: Some(wire.root()),
            blocks: VecDeque::new(),
            bytes: 0,
            range_next_slot,
            range_start_slot: range_next_slot.map(|_| wire.slot()),
        });
        if branch.peer != peer ||
            branch.next_root != Some(wire.root()) ||
            branch.blocks.front().is_some_and(|child| {
                let parent_mismatch = child.parent_root() != wire.root();
                parent_mismatch || child.slot() <= wire.slot()
            }) ||
            branch.blocks.iter().any(|known| known.root() == wire.root()) ||
            branch.blocks.len() >= MAX_ANCESTRY_BLOCKS ||
            branch
                .bytes
                .checked_add(wire.ssz().len())
                .is_none_or(|bytes| bytes > MAX_ANCESTRY_BYTES)
        {
            self.branch = None;
            self.remove_peer(peer);
            return Ok(());
        }
        branch.bytes += wire.ssz().len();
        branch.next_root = Some(wire.parent_root());
        branch.blocks.push_front(wire);
        Ok(())
    }

    fn expire(&mut self) -> Result<(), LiveSyncError> {
        self.retire_expired_data_provider()?;
        if self
            .broker_envelope
            .as_ref()
            .is_some_and(|pending| pending.started.elapsed() >= REQUEST_TIMEOUT)
        {
            return Err(LiveSyncError::RequestTimeout);
        }
        if self
            .sidecars
            .requested()
            .is_some_and(|pending| pending.started.elapsed() >= REQUEST_TIMEOUT)
        {
            // The private transport request can still own a stream. Stop rather than reuse its
            // admission or issue a duplicate request whose late response could be confused.
            return Err(LiveSyncError::RequestTimeout);
        }
        if self
            .block_request
            .as_ref()
            .is_some_and(|pending| pending.started.elapsed() >= REQUEST_TIMEOUT) ||
            self.envelopes
                .requested()
                .is_some_and(|pending| pending.started.elapsed() >= REQUEST_TIMEOUT)
        {
            return Err(LiveSyncError::RequestTimeout);
        }
        Ok(())
    }
    /// Called only after matching the exact completed request. Temporary server errors
    /// retain their connection; resource-unavailable providers release a replacement slot.
    fn retire_unavailable_provider(&mut self, peer: PeerId, code: u8) -> Result<(), LiveSyncError> {
        if code == 3 {
            self.remove_peer(peer);
            self.peer.as_mut().ok_or(LiveSyncError::PeerUnavailable)?.retire_peer(peer)?;
        }
        Ok(())
    }
    fn remove_peer(&mut self, peer: PeerId) {
        if self.branch.as_ref().is_some_and(|branch| branch.peer == peer) {
            self.branch = None;
        }
        self.targets.retain(|target| target.peer != peer);
    }
    fn range_target(&self) -> Option<&Target> {
        self.targets.iter().find(|target| {
            target.status.head_slot >= self.cursor &&
                target.earliest.is_none_or(|slot| slot <= self.cursor)
        })
    }
    fn advance_range_cursor(&mut self, next_slot: u64) {
        // This frontier comes only from a matched, strictly admitted range input. It remains
        // a retrieval hint and never supplies canonical head, finality or execution authority.
        self.validated_range_cursor = self.validated_range_cursor.max(next_slot);
        self.cursor = self.cursor.max(self.validated_range_cursor);
    }
    fn rebase_cursor(&mut self, canonical_slot: u64) -> Result<(), LiveSyncError> {
        let canonical_next = canonical_slot.checked_add(1).ok_or(LiveSyncError::Bounds)?;
        // Peer churn discards unverified empty-slot skips while preserving admitted ranges.
        self.cursor = self.validated_range_cursor.max(canonical_next);
        Ok(())
    }
    const fn reset_cursor(&mut self) -> Result<(), LiveSyncError> {
        // Query the private actual owner before the next request; never rewind to a stale seed.
        self.rebase = true;
        Ok(())
    }
}

/// Dropping or requesting cancellation stops actual peer IO; it does not override import validity.
#[derive(Debug)]
pub struct LiveSyncCancellation {
    stop: Option<oneshot::Sender<()>>,
}
impl LiveSyncCancellation {
    /// Request cooperative shutdown of the one peer owner.
    pub fn cancel(mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }
}
/// Failures from the actual bounded peer, retrieval and strict import owners.
#[derive(Debug, thiserror::Error)]
pub enum LiveSyncError {
    /// Concrete component configuration bindings disagree.
    #[error("live sync component configuration mismatch")]
    Configuration,
    /// Bounded scheduling count, ancestry or integer limit failed.
    #[error("live sync scheduling bound exceeded")]
    Bounds,
    /// A reply did not correspond to the exact outstanding original request.
    #[error("live sync original response association mismatch")]
    Response,
    /// The owned peer failed during bounded blocking identity publication.
    #[error("live sync peer owner unavailable")]
    PeerUnavailable,
    /// The availability owner closed unexpectedly.
    #[error("live sync availability broker closed")]
    BrokerClosed,
    /// An actual transport request expired while its outcome remained unresolved.
    #[error("live sync transport request timed out")]
    RequestTimeout,
    /// The admitted owner cannot grant further graph authority.
    #[error("live sync import owner halted")]
    ImporterHalted,
    /// Actual peer startup, transport or identity operation failed.
    #[error(transparent)]
    Peer(#[from] PeerStartupError),
    /// Strict import or uncertain publication failed.
    #[error(transparent)]
    Import(#[from] ImportError),
    /// Mandatory actual candidate availability failed.
    #[error(transparent)]
    Retrieval(#[from] RetrievalError),
}
struct QueuedGossip {
    peer: PeerId,
    id: GossipRequestId,
    input: GossipInput,
}
struct ActiveGossip {
    id: GossipRequestId,
    reply: Pin<Box<dyn Future<Output = Result<GossipVerdict, ImportError>> + Send>>,
    _input: GossipInput,
}
struct BranchRecovery {
    peer: PeerId,
    target_root: Root,
    next_root: Option<Root>,
    blocks: VecDeque<WireBlock>,
    bytes: usize,
    range_next_slot: Option<u64>,
    range_start_slot: Option<u64>,
}
struct Target {
    peer: PeerId,
    status: Status,
    earliest: Option<u64>,
    examined_root: Option<Root>,
}
struct BlockPending {
    peer: PeerId,
    id: BlockRequestId,
    request: BlockRequest,
    started: Instant,
}
struct EnvelopePending {
    peer: PeerId,
    id: EnvelopeRequestId,
    root: Root,
    slot: u64,
    metadata: EnvelopeBlockReference,
    candidate: CandidateGuard,
    started: Instant,
    tried: Vec<PeerId>,
}
struct SidecarPending {
    peer: PeerId,
    id: SidecarRequestId,
    job: RetrievalJob,
    started: Instant,
    tried: Vec<PeerId>,
}
struct ActiveImport {
    peer: PeerId,
    root: Root,
    slot: u64,
    kind: ImportKind,
    reply: Pin<Box<dyn Future<Output = Result<ImportReceipt, ImportError>> + Send>>,
    _batch: RetainedBatch,
    _candidate: CandidateGuard,
    gossip: Option<GossipRequestId>,
    range_next_slot: Option<u64>,
}
enum ImportKind {
    Block { is_gloas: bool, metadata: EnvelopeBlockReference },
    Envelope,
}
enum RetainedBatch {
    Blocks { _batch: BlockBatch },
    Envelopes { _batch: EnvelopeBatch },
    Branch { _wire: WireBlock },
    Gossip { _input: GossipInput },
}
enum DriverEvent {
    Peer(PeerRuntimeEvent),
    Retrieval(RetrievalJob),
    EnvelopeRetrieval(EnvelopeRetrievalJob),
    HistoricalRetrieval(availability::history_retrieval::HistoricalBlockJob),
    Imported(Result<ImportReceipt, ImportError>),
    Tick,
    Gossip(Result<GossipVerdict, ImportError>),
    Served(Result<Option<ServingBatch>, ImportError>),
}
/// Local discovery backpressure defers a query; all other errors retain their original failure.
fn discovery_scheduled(result: Result<(), PeerStartupError>) -> Result<bool, LiveSyncError> {
    match result {
        Ok(()) => Ok(true),
        Err(PeerStartupError::Peer(PeerRuntimeError::Discovery(DiscoveryError::Capacity))) => {
            tracing::debug!(target: "beacon::peers",
                "[beacon] Deferred discovery query while its bounded queue is occupied");
            Ok(false)
        }
        Err(error) => Err(error.into()),
    }
}

fn now() -> Result<u64, LiveSyncError> {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| LiveSyncError::Configuration)?
            .as_millis(),
    )
    .map_err(|_| LiveSyncError::Bounds)
}

mod startup;

#[cfg(test)]
pub(crate) mod tests;
