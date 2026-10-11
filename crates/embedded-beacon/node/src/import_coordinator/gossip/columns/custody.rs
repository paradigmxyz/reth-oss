//! Bounded local custody acquisition against the actual unsigned native anchor.
//!
//! These originals acquire only proof-input pins. They cannot insert graph roots, acknowledge
//! execution, or bypass the gossip seen policy for a finalized Fulu block.

use super::*;
use crate::live_sync::availability::serving::{verify_original, verify_unsigned};
use reth_embedded_beacon_network::block_rpc::BlockRequest;
use std::collections::BTreeSet;

impl ImportCoordinator {
    pub(crate) fn retain_anchor_columns(
        &self,
        root: Root,
        originals: Vec<WireSidecar>,
    ) -> impl Future<Output = Result<(), ImportError>> + Send + 'static {
        let admitted = self.try_anchor_columns(root, originals);
        async move { admitted?.await.map_err(|_| ImportError::Closed)? }
    }
    fn try_anchor_columns(
        &self,
        root: Root,
        originals: Vec<WireSidecar>,
    ) -> Result<oneshot::Receiver<Result<(), ImportError>>, ImportError> {
        if self.is_halted() {
            return Err(ImportError::Halted)
        }
        if originals.is_empty() || originals.len() > 4 {
            return Err(ImportError::Bounds)
        }
        let bytes = originals
            .iter()
            .try_fold(0usize, |total, wire| total.checked_add(wire.ssz().len()))
            .ok_or(ImportError::Bounds)?;
        if bytes == 0 || bytes > self.limits.max_queued_bytes {
            return Err(ImportError::Bounds)
        }
        let bytes = u32::try_from(bytes).map_err(|_| ImportError::Bounds)?;
        let bytes =
            self.bytes.clone().try_acquire_many_owned(bytes).map_err(|error| match error {
                tokio::sync::TryAcquireError::Closed => ImportError::Halted,
                tokio::sync::TryAcquireError::NoPermits => ImportError::Backpressure,
            })?;
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
        queue.send(Command::CustodyColumns { root, originals, reply, _bytes: bytes, _slot: slot });
        Ok(receive)
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
    pub(in crate::import_coordinator) fn retain_custody_columns(
        &self,
        root: Root,
        originals: Vec<WireSidecar>,
    ) -> Result<(), ImportError> {
        // A genuinely signed original freshly served by this owner takes precedence over
        // its unsigned bootstrap copy, including historical Deneb/Electra blob contexts.
        let signed = match self.serve(ServingRequest::Blocks(BlockRequest::Roots(vec![root])))? {
            Some(ServingBatch::Blocks(mut blocks)) => blocks
                .pop()
                .map(|wire| {
                    if wire.root() != root {
                        return Err(ImportError::Wire)
                    }
                    SignedBeaconBlock::from_ssz(wire.ssz(), wire.fork(), preset(&self.config))
                        .map_err(|_| ImportError::Wire)
                })
                .transpose()?,
            _ => None,
        };
        let anchor = if signed.is_none() { self.custody_anchor(root)? } else { None };
        if signed.is_none() && anchor.is_none() {
            return Err(ImportError::GossipIgnore("native custody context unavailable"))
        }
        let mut indices = BTreeSet::new();
        if originals.is_empty() || originals.len() > 4 {
            return Err(ImportError::Bounds)
        }
        for wire in &originals {
            let digest = self
                .config
                .fork_digest(wire.slot() / self.config.slots_per_epoch)
                .map_err(|_| ImportError::Configuration)?;
            if wire.block_root() != root ||
                !indices.insert(wire.index()) ||
                wire.context() != digest
            {
                return Err(ImportError::Wire)
            }
        }
        // Verify the entire batch before publishing any independent proof-input owner.
        let verified = if let Some(anchor) = &anchor {
            verify_unsigned(anchor, &originals, &self.config)
        } else {
            verify_original(
                signed.as_ref().ok_or(ImportError::Wire)?,
                root,
                &originals,
                &self.config,
            )
        };
        verified.map_err(|error| match error {
            RetrievalError::Missing => {
                ImportError::GossipIgnore("native custody signature context unavailable")
            }
            RetrievalError::Configuration => ImportError::Configuration,
            _ => ImportError::GossipReject("invalid native anchor column proof"),
        })?;
        let publisher = self.column_retention.as_ref().ok_or(ImportError::Backpressure)?;
        for original in originals {
            let receipt =
                ValidatedColumn { original, config_digest: checked_config_digest(&self.config)? };
            publisher.retain(&receipt)?;
        }
        Ok(())
    }
}
