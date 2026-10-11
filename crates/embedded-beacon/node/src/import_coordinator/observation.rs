//! Read-only observations from the authenticated graph owner and exact original encodings.
//! Saved records outside retained graph membership cannot supply API canonicality or finality.

use super::*;
use reth_embedded_beacon_fork_choice::{
    compact::CompactGraphStats, ExecutionStatus, ForkChoiceNode,
};
use reth_embedded_beacon_primitives::BeaconBlockHeader;
use reth_embedded_beacon_state_transition::ssz::json::JsonSsz;
use reth_embedded_beacon_storage::CompactStateStats;

/// Maximum complete serialized observation, independent of HTTP connection admission.
pub const MAX_OBSERVATION_BYTES: usize = 32 * 1024 * 1024;
const MAX_STATE_ROOT_SEARCH: usize = 2048;

/// Exact graph object selector; a numeric slot never aliases an earlier skipped slot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ObservationId {
    /// Current graph-derived head.
    Head,
    /// Current realized finalized block.
    Finalized,
    /// Exact canonical block slot.
    Slot(u64),
    /// Exact retained root; state-root queries also accept the actual post-state commitment.
    Root(Root),
}

/// Read-only owner query, admitted with the existing immediate command-count backpressure.
#[derive(Clone, Copy, Debug)]
pub enum ObservationRequest {
    /// Actual graph clock, canonical selection and bounded state allocation measurements.
    Status,
    /// Original signed beacon block or its original signed header.
    Block {
        /// Exact retained graph selector.
        id: ObservationId,
        /// Serialize its original signed header instead of the complete body.
        header_only: bool,
    },
    /// Original signed Gloas execution envelope, only when actually admitted and retained.
    Envelope(ObservationId),
    /// Complete post-state commitment already checked by the strict admitted graph.
    StateRoot(ObservationId),
}

/// Copy of actual state and execution acknowledgement, without mutable validity controls.
#[derive(Clone, Debug)]
pub struct OwnerObservation {
    /// Exact graph snapshot at this command boundary.
    pub snapshot: StoreSnapshot,
    /// Actual owner clock slot.
    pub current_slot: u64,
    /// True only for an actual same-adapter receipt for the current canonical execution hashes.
    pub execution_acknowledged: bool,
    /// Whether the actual typed Engine adapter has closed.
    pub engine_closed: bool,
    /// Actual undecided/optimistic canonical head branch.
    pub head_optimistic: bool,
    /// Real retained graph allocation measurements.
    pub graph: CompactGraphStats,
    /// Real compact source allocation measurements; active leases remain counted.
    pub source: CompactStateStats,
    /// Actual owner work counters, with no validity inference from telemetry.
    pub imports: ImportObservation,
}

/// Exact original object selected by the graph, with metadata computed at the same boundary.
#[derive(Clone, Debug)]
pub struct ObjectObservation {
    /// Native schema fork, never inferred from requested endpoint strings.
    pub fork: Fork,
    /// Actual retained unsigned block root.
    pub block_root: Root,
    /// Checked native block header.
    pub header: BeaconBlockHeader,
    /// Whether this block belongs to the actual selected head ancestry.
    pub canonical: bool,
    /// Whether this block belongs to the realized finalized ancestry.
    pub finalized: bool,
    /// True while this exact canonical payload branch is undecided or execution is optimistic.
    pub execution_optimistic: bool,
    /// Complete bounded native JSON, absent only for state-root observations.
    pub json: Vec<u8>,
}

/// No-data is distinct from infrastructure failure or a halted importer.
#[derive(Clone, Debug)]
pub enum Observation {
    /// Actual current owner measurements.
    Status(OwnerObservation),
    /// An original object is available only through authenticated retained graph membership.
    Object(Option<ObjectObservation>),
}

impl ImportCoordinator {
    /// Current command/input reservations, including active work; no queued waiter is inferred.
    /// These resource observations may reflect adjacent owner turns and grant no admission
    /// authority.
    pub fn reserved_resources(&self) -> (usize, usize) {
        (
            self.limits.command_capacity.saturating_sub(self.commands.available_permits()),
            self.limits.max_queued_bytes.saturating_sub(self.bytes.available_permits()),
        )
    }
    /// Submit immediately to the existing bounded owner queue; no auxiliary waiter list exists.
    pub fn observe(
        &self,
        request: ObservationRequest,
    ) -> impl Future<Output = Result<Observation, ImportError>> + Send + 'static {
        let admitted = self.try_observe(request);
        async move { admitted?.await.map_err(|_| ImportError::Closed)? }
    }

    fn try_observe(
        &self,
        request: ObservationRequest,
    ) -> Result<oneshot::Receiver<Result<Observation, ImportError>>, ImportError> {
        if self.is_halted() {
            return Err(ImportError::Halted)
        }
        let slot = self.commands.clone().try_acquire_owned().map_err(|error| match error {
            tokio::sync::TryAcquireError::Closed => ImportError::Halted,
            tokio::sync::TryAcquireError::NoPermits => ImportError::Backpressure,
        })?;
        let queue = self.sender.try_reserve().map_err(|error| match error {
            mpsc::error::TrySendError::Closed(()) => ImportError::Closed,
            mpsc::error::TrySendError::Full(()) => ImportError::Backpressure,
        })?;
        let (reply, receive) = oneshot::channel();
        self.observe_queue_admission();
        queue.send(Command::Observe { request, reply, _slot: slot });
        Ok(receive)
    }
}

impl Graph {
    fn observation_header(
        &self,
        root: Root,
    ) -> Result<Option<(Fork, BeaconBlockHeader)>, ImportError> {
        match self {
            Self::Classic { store, .. } => store
                .block(root)
                .map(|block| block.header().map(|header| (block.schema_fork(), header)))
                .transpose()
                .map_err(|_| ImportError::Wire),
            Self::Gloas { store, .. } => {
                if let Some(block) = store.block(root) {
                    let preset = ReadGloasGossipView::store_parameters(store).phase0.preset;
                    return Ok(Some((
                        Fork::Gloas,
                        block.header(preset).map_err(|_| ImportError::Wire)?,
                    )))
                }
                ReadGloasGossipView::historical_block(store, root)
                    .map(|block| block.header().map(|header| (block.schema_fork(), header)))
                    .transpose()
                    .map_err(|_| ImportError::Wire)
            }
        }
    }

    fn observation_ancestor(
        &self,
        node: ForkChoiceNode,
        slot: u64,
    ) -> Result<ForkChoiceNode, ImportError> {
        Ok(match self {
            Self::Classic { store, .. } => ForkChoiceNode {
                root: ReadGossipView::get_ancestor(store, node.root, slot)?,
                payload_status: None,
            },
            Self::Gloas { store, .. } => ReadGloasGossipView::get_ancestor(store, node, slot)?,
        })
    }

    fn observation_select(
        &self,
        id: ObservationId,
        snapshot: StoreSnapshot,
    ) -> Result<Option<Root>, ImportError> {
        let root = match id {
            ObservationId::Head => snapshot.head.root,
            ObservationId::Finalized => snapshot.finalized_checkpoint.root,
            ObservationId::Root(root) => root,
            ObservationId::Slot(slot) => {
                if slot > snapshot.head_slot {
                    return Ok(None)
                }
                // API lookup stops at actual retained ancestry; strict consensus ancestry remains
                // unchanged.
                let mut candidate = snapshot.head.root;
                let mut found = None;
                for _ in 0..MAX_STATE_ROOT_SEARCH {
                    let Some((_, header)) = self.observation_header(candidate)? else { break };
                    if header.slot <= slot {
                        found = (header.slot == slot).then_some(candidate);
                        break
                    }
                    if header.parent_root == [0; 32] || header.parent_root == candidate {
                        break
                    }
                    candidate = header.parent_root;
                }
                let Some(found) = found else { return Ok(None) };
                found
            }
        };
        Ok(self.observation_header(root)?.map(|_| root))
    }

    fn observation_flags(
        &self,
        root: Root,
        header: BeaconBlockHeader,
        snapshot: StoreSnapshot,
    ) -> Result<(bool, bool, bool), ImportError> {
        let canonical_node = self.observation_ancestor(snapshot.head, header.slot)?;
        let canonical = canonical_node.root == root;
        let finalized = if let Some((_, final_header)) =
            self.observation_header(snapshot.finalized_checkpoint.root)?
        {
            header.slot <= final_header.slot &&
                canonical &&
                self.observation_ancestor(snapshot.head, final_header.slot)?.root ==
                    snapshot.finalized_checkpoint.root
        } else {
            false
        };
        let optimistic = match self {
            Self::Classic { store, .. } => {
                ReadGossipView::execution_status(store, root) != ExecutionStatus::Valid
            }
            Self::Gloas { store, .. } => {
                if canonical {
                    match canonical_node.payload_status {
                        Some(PayloadStatus::Empty) => false,
                        Some(PayloadStatus::Pending) => true,
                        _ => {
                            ReadGloasGossipView::execution_status(store, root) !=
                                ExecutionStatus::Valid
                        }
                    }
                } else {
                    ReadGloasGossipView::execution_status(store, root) != ExecutionStatus::Valid
                }
            }
        };
        Ok((canonical, finalized, optimistic))
    }
}

impl<E, C, G> Worker<E, C, G>
where
    E: ExecutionEngine<
            Payload = ExecutionData,
            PayloadReply = PayloadReply,
            Forkchoice = ForkchoiceState,
            ForkchoiceReply = ForkchoiceReply,
        > + Clone,
    E::Error: std::error::Error + Send + Sync + 'static,
    C: ClassicAvailability,
    G: GloasAvailability,
{
    pub(super) fn observe(&self, request: ObservationRequest) -> Result<Observation, ImportError> {
        let snapshot = self.graph.snapshot()?;
        if matches!(request, ObservationRequest::Status) {
            let graph = match &self.graph {
                Graph::Classic { store, .. } => store.stats()?,
                Graph::Gloas { store, .. } => store.stats()?,
            };
            let expected =
                self.graph.execution_state_with_boundary(self.execution_boundary.as_ref())?;
            let execution_acknowledged = expected.map_or(
                snapshot.head.payload_status.is_none() &&
                    self.config
                        .fork_at_slot(snapshot.head_slot)
                        .map_err(|_| ImportError::Configuration)?
                        .fork <
                        Fork::Bellatrix,
                |(_, state)| {
                    self.last_verified_forkchoice
                        .as_ref()
                        .is_some_and(|receipt| receipt.matches(&self.engine, state))
                },
            );
            return Ok(Observation::Status(OwnerObservation {
                snapshot,
                current_slot: self.graph.current_slot(),
                execution_acknowledged,
                engine_closed: self.engine.is_closed(),
                imports: self.import_metrics,
                head_optimistic: self
                    .graph
                    .observation_header(snapshot.head.root)?
                    .map(|(_, header)| {
                        self.graph.observation_flags(snapshot.head.root, header, snapshot)
                    })
                    .transpose()?
                    .is_none_or(|(_, _, optimistic)| optimistic),
                graph,
                source: self
                    .repository
                    .stats()
                    .map_err(|error| ImportError::Persistence(error.to_string()))?,
            }))
        }
        let id = match request {
            ObservationRequest::Block { id, .. } |
            ObservationRequest::Envelope(id) |
            ObservationRequest::StateRoot(id) => id,
            ObservationRequest::Status => unreachable!(),
        };
        let mut root = self.graph.observation_select(id, snapshot)?;
        if root.is_none() &&
            let ObservationRequest::StateRoot(ObservationId::Root(state_root)) = request
        {
            let mut candidate = snapshot.head.root;
            for _ in 0..MAX_STATE_ROOT_SEARCH {
                let Some((_, header)) = self.graph.observation_header(candidate)? else { break };
                if header.state_root == state_root {
                    root = Some(candidate);
                    break
                }
                if header.parent_root == [0; 32] || header.parent_root == candidate {
                    break
                }
                candidate = header.parent_root;
            }
        }
        let Some(root) = root else { return Ok(Observation::Object(None)) };
        let Some((fork, header)) = self.graph.observation_header(root)? else {
            return Ok(Observation::Object(None))
        };
        let (canonical, finalized, execution_optimistic) =
            self.graph.observation_flags(root, header, snapshot)?;
        let json = match request {
            ObservationRequest::StateRoot(_) => {
                // Acquire a real owning reconstruction lease; lost records or pins cannot appear
                // available.
                let Some(lease) = self
                    .repository
                    .block_state(root)
                    .map_err(|error| ImportError::Persistence(error.to_string()))?
                else {
                    return Ok(Observation::Object(None))
                };
                if lease.state().hash_tree_root().map_err(|_| ImportError::Wire)? !=
                    header.state_root
                {
                    return Err(ImportError::Wire)
                }
                Vec::new()
            }
            ObservationRequest::Block { header_only, .. } => {
                let Some((format, bytes)) = self
                    .repository
                    .original_block(root)
                    .map_err(|error| ImportError::Persistence(error.to_string()))?
                else {
                    return Ok(Observation::Object(None))
                };
                if format != CheckpointBlockFormat::Signed {
                    return Ok(Observation::Object(None))
                }
                let signed = SignedBeaconBlock::from_ssz(&bytes, fork, preset(&self.config))
                    .map_err(|_| ImportError::Wire)?;
                if signed.header().map_err(|_| ImportError::Wire)? != header ||
                    signed.message_root().map_err(|_| ImportError::Wire)? != root
                {
                    return Err(ImportError::Wire)
                }
                if header_only {
                    let signature = match &signed {
                        SignedBeaconBlock::Phase0(value, _) => value.signature,
                        SignedBeaconBlock::Altair(value, _) => value.signature,
                        SignedBeaconBlock::Bellatrix(value, _) => value.signature,
                        SignedBeaconBlock::Capella(value, _) => value.signature,
                        SignedBeaconBlock::Deneb(value, _) => value.signature,
                        SignedBeaconBlock::Electra(value, _) => value.signature,
                        SignedBeaconBlock::Fulu(value, _) => value.signature,
                        SignedBeaconBlock::Gloas(value, _) => value.signature,
                    };
                    let header_json = header
                        .to_beacon_json(preset(&self.config), 4096)
                        .map_err(|_| ImportError::Bounds)?;
                    let signature_json = signature
                        .to_beacon_json(preset(&self.config), 256)
                        .map_err(|_| ImportError::Bounds)?;
                    let mut output = b"{\"message\":".to_vec();
                    output.extend_from_slice(&header_json);
                    output.extend_from_slice(b",\"signature\":");
                    output.extend_from_slice(&signature_json);
                    output.push(b'}');
                    output
                } else {
                    signed
                        .to_beacon_json(preset(&self.config), MAX_OBSERVATION_BYTES)
                        .map_err(|_| ImportError::Bounds)?
                }
            }
            ObservationRequest::Envelope(_) => {
                // A saved FULL object cannot establish the selected branch's FULL decision.
                let selected = self.graph.observation_ancestor(snapshot.head, header.slot)?;
                let retained_full = match &self.graph {
                    Graph::Gloas { store, .. } => {
                        ReadGloasGossipView::payload(store, root).is_some()
                    }
                    Graph::Classic { .. } => false,
                };
                if selected.root != root ||
                    selected.payload_status != Some(PayloadStatus::Full) ||
                    !retained_full
                {
                    return Ok(Observation::Object(None))
                }
                if fork != Fork::Gloas || execution_optimistic {
                    return Ok(Observation::Object(None))
                }
                let Some(bytes) = self
                    .repository
                    .original_envelope(root)
                    .map_err(|error| ImportError::Persistence(error.to_string()))?
                else {
                    return Ok(Observation::Object(None))
                };
                let envelope =
                    gloas::SignedExecutionPayloadEnvelope::from_ssz(&bytes, preset(&self.config))
                        .map_err(|_| ImportError::Wire)?;
                if envelope.message.beacon_block_root != root ||
                    envelope.message.payload.slot_number != header.slot
                {
                    return Err(ImportError::Wire)
                }
                envelope
                    .to_beacon_json(preset(&self.config), MAX_OBSERVATION_BYTES)
                    .map_err(|_| ImportError::Bounds)?
            }
            ObservationRequest::Status => unreachable!(),
        };
        Ok(Observation::Object(Some(ObjectObservation {
            fork,
            block_root: root,
            header,
            canonical,
            finalized,
            execution_optimistic,
            json,
        })))
    }
}
