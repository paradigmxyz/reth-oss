//! Column gossip against an actual strictly admitted original block.
//!
//! Unknown blocks are retrievable dependencies, never fabricated validation contexts. Exact
//! original native proposer validation plus fresh inclusion/KZG checks precede seen insertion.

use super::*;
use crate::live_sync::availability::{serving::verify_original, RetrievalError};
use c_kzg::ethereum_kzg_settings;
use reth_embedded_beacon_data_availability::admission::verify_gloas_sampling_batch;
use reth_embedded_beacon_network::{availability_rpc::WireSidecar, Protocol};

mod custody;

/// Local proof receipt minted only after this owner's actual native gossip kernels succeed.
/// Original fields remain private; a raw peer object or a public verdict cannot construct it.
#[derive(Debug)]
pub(crate) struct ValidatedColumn {
    original: WireSidecar,
    config_digest: Root,
}
impl ValidatedColumn {
    pub(crate) const fn original(&self) -> &WireSidecar {
        &self.original
    }
    pub(crate) const fn config_digest(&self) -> Root {
        self.config_digest
    }
}

/// Optional local persistence receives the actual sealed native validation result.
pub(crate) trait ColumnRetention: std::fmt::Debug + Send + Sync {
    fn config_digest(&self) -> Root;
    fn retain(&self, receipt: &ValidatedColumn) -> Result<(), ImportError>;
    /// Opaque retention implementations must explicitly establish healthy maintenance state.
    fn ensure_healthy(&self) -> Result<(), ImportError> {
        Err(ImportError::RecoveryRequired)
    }
}
impl ImportCoordinator {
    pub(crate) fn set_column_retention(
        &self,
        publisher: Arc<dyn ColumnRetention>,
    ) -> impl Future<Output = Result<(), ImportError>> + Send + 'static {
        let admitted = self.try_column_retention(publisher);
        async move { admitted?.await.map_err(|_| ImportError::Closed)? }
    }
    fn try_column_retention(
        &self,
        publisher: Arc<dyn ColumnRetention>,
    ) -> Result<oneshot::Receiver<Result<(), ImportError>>, ImportError> {
        if self.is_halted() {
            return Err(ImportError::Halted)
        }
        if publisher.config_digest() != self.config_digest {
            return Err(ImportError::Configuration)
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
        queue.send(Command::ColumnRetention { publisher, reply, _slot: slot });
        Ok(receive)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum ColumnIdentity {
    Fulu { slot: u64, proposer: u64, column: u64 },
    Gloas { root: Root, column: u64 },
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
    pub(super) fn validate_column_gossip(
        &self,
        observations: &mut GossipState,
        topic: GossipTopic,
        bytes: &[u8],
        time: u64,
    ) -> Result<ValidatedColumn, Decision> {
        let GossipKind::Column(subnet) = topic.kind else { return Err(Decision::Ignore) };
        let wire = WireSidecar::from_ssz(
            bytes.to_vec(),
            topic.digest,
            Protocol::DataColumnsByRootV1,
            &self.config,
        )
        .map_err(|_| Decision::Reject)?;
        // The normative duplicate identity precedes subnet, time and dependency checks.
        // Gloas has no slot in its identity: changing a seen object's slot remains Ignore.
        let key = match wire.fork() {
            Fork::Fulu => {
                let sidecar =
                    auxiliary::fulu::DataColumnSidecar::from_ssz(bytes, preset(&self.config))
                        .map_err(|_| Decision::Reject)?;
                ColumnIdentity::Fulu {
                    slot: sidecar.signed_block_header.message.slot,
                    proposer: sidecar.signed_block_header.message.proposer_index,
                    column: wire.index(),
                }
            }
            Fork::Gloas => ColumnIdentity::Gloas { root: wire.block_root(), column: wire.index() },
            _ => return Err(Decision::Reject),
        };
        if observations.columns.contains_key(&key) || observations.columns.len() >= 16_384 {
            return Err(Decision::Ignore)
        }
        if wire.fork() != topic.fork || wire.index() != subnet {
            return Err(Decision::Reject)
        }
        let start = self
            .config
            .seconds_per_slot
            .checked_mul(wire.slot())
            .and_then(|time| time.checked_add(self.config.genesis_time))
            .and_then(|time| time.checked_mul(1000))
            .ok_or(Decision::Ignore)?;
        if time.checked_add(500).ok_or(Decision::Ignore)? < start {
            return Err(Decision::Ignore)
        }
        // Membership and original signature authority come from the live native graph only.
        let native = match &self.graph {
            Graph::Classic { store, .. } => store.block(wire.block_root()).cloned(),
            Graph::Gloas { store, .. } => store
                .block(wire.block_root())
                .map(|block| {
                    BeaconBlock::Gloas(
                        block.clone(),
                        ReadGloasGossipView::store_parameters(store).phase0.preset,
                    )
                })
                .or_else(|| {
                    ReadGloasGossipView::historical_block(store, wire.block_root()).cloned()
                }),
        }
        .ok_or(Decision::Ignore)?;
        let header = native.header().map_err(|_| Decision::Reject)?;
        if header.slot != wire.slot() ||
            native.hash_tree_root().map_err(|_| Decision::Reject)? != wire.block_root()
        {
            return Err(Decision::Reject)
        }
        check_slot(&self.config, topic, header.slot)?;
        if topic.fork == Fork::Fulu {
            let finalized =
                self.graph.snapshot().map_err(Decision::Infrastructure)?.finalized_checkpoint;
            if header.slot <= finalized.epoch.saturating_mul(self.config.slots_per_epoch) {
                return Err(Decision::Ignore)
            }
            let ancestor = match &self.graph {
                Graph::Classic { store, .. } => {
                    ReadGossipView::get_checkpoint_block(store, header.parent_root, finalized.epoch)
                        .map_err(store_error)?
                }
                Graph::Gloas { store, .. } => {
                    let parent = ReadGloasGossipView::node_for_root(store, header.parent_root)
                        .map_err(store_error)?;
                    ReadGloasGossipView::get_ancestor(
                        store,
                        parent,
                        finalized.epoch.saturating_mul(self.config.slots_per_epoch),
                    )
                    .map_err(store_error)?
                    .root
                }
            };
            if ancestor != finalized.root {
                return Err(Decision::Reject)
            }
            // A native admitted exact block has already passed actual proposer BLS, expected
            // proposer, post-state, parent-slot and finalized-ancestry checks. Matching its full
            // body/header/signature below reuses that proof without re-running a transition.
        }
        if let BeaconBlock::Gloas(block, native_preset) = &native {
            // Gloas columns have no proposer-header signature. The actual retained native
            // block itself is their commitment context, including genuine unsigned anchors.
            let sidecar = auxiliary::gloas::DataColumnSidecar::from_ssz(bytes, *native_preset)
                .map_err(|_| Decision::Reject)?;
            verify_gloas_sampling_batch(
                ethereum_kzg_settings(0),
                wire.block_root(),
                block,
                &[sidecar],
                *native_preset,
                &[wire.index()],
            )
            .map_err(|_| Decision::Reject)?;
            observations.columns.insert(key, wire.slot());
            return Ok(ValidatedColumn {
                original: wire,
                config_digest: checked_config_digest(&self.config)
                    .map_err(|_| Decision::Infrastructure(ImportError::Configuration))?,
            })
        }
        let Some((format, original)) =
            self.repository.original_block(wire.block_root()).map_err(|error| {
                Decision::Infrastructure(ImportError::Persistence(error.to_string()))
            })?
        else {
            return Err(Decision::Ignore)
        };
        if format != CheckpointBlockFormat::Signed {
            return Err(Decision::Ignore)
        }
        let signed = SignedBeaconBlock::from_ssz(&original, topic.fork, preset(&self.config))
            .map_err(|_| Decision::Infrastructure(ImportError::Wire))?;
        if signed.message_root().map_err(|_| Decision::Reject)? != wire.block_root() {
            return Err(Decision::Infrastructure(ImportError::Wire))
        }
        verify_original(&signed, wire.block_root(), std::slice::from_ref(&wire), &self.config)
            .map_err(|error| match error {
                RetrievalError::Configuration |
                RetrievalError::Storage |
                RetrievalError::Capacity |
                RetrievalError::Closed |
                RetrievalError::Timeout => Decision::Infrastructure(
                    ImportError::PublicationInfrastructure(error.to_string()),
                ),
                RetrievalError::Missing => Decision::Ignore,
                _ => Decision::Reject,
            })?;
        observations.columns.insert(key, wire.slot());
        Ok(ValidatedColumn {
            original: wire,
            config_digest: checked_config_digest(&self.config)
                .map_err(|_| Decision::Infrastructure(ImportError::Configuration))?,
        })
    }
}
