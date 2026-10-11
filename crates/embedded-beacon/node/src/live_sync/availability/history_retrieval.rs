//! Historical retrieval preserves bounded wire leases and actual native-context publication.

use super::*;
use crate::import_coordinator::{HistoricalDependency, ImportCoordinator, ImportError};
use reth_embedded_beacon_network::block_rpc::BlockBatch;

#[derive(Debug)]
pub(crate) struct HistoricalBlockJob {
    dependency: HistoricalDependency,
    created: Instant,
    reply: oneshot::Sender<Result<BlockBatch, RetrievalError>>,
    _permit: OwnedSemaphorePermit,
}
impl HistoricalBlockJob {
    pub(crate) const fn root(&self) -> Root {
        self.dependency.root()
    }
    pub(crate) const fn upper_slot(&self) -> u64 {
        self.dependency.upper_slot()
    }
    pub(crate) fn closed(&self) -> bool {
        self.reply.is_closed()
    }
    pub(crate) const fn created(&self) -> Instant {
        self.created
    }
    pub(crate) fn complete(self, result: Result<BlockBatch, RetrievalError>) {
        let Self { reply, _permit, .. } = self;
        drop(_permit);
        let _ = reply.send(result);
    }
}
impl RetrievalAvailability {
    pub(super) async fn acquire_historical_parent(
        &self,
        importer: &ImportCoordinator,
    ) -> Result<bool, ImportError> {
        let Some(dependency) = importer.custody_dependency().await? else { return Ok(false) };
        let permit = match self.history_requests.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => return Ok(false),
        };
        let queue = match self.history_sender.try_reserve() {
            Ok(queue) => queue,
            Err(_) => return Ok(false),
        };
        let (reply, receive) = oneshot::channel();
        queue.send(HistoricalBlockJob {
            dependency,
            created: Instant::now(),
            reply,
            _permit: permit,
        });
        let batch = match tokio::time::timeout(RETRIEVAL_TIMEOUT, receive).await {
            Ok(Ok(Ok(batch))) => batch,
            Ok(Ok(Err(
                RetrievalError::Missing | RetrievalError::Capacity | RetrievalError::Timeout,
            ))) |
            Err(_) => return Ok(false),
            Ok(Ok(Err(error))) => return Err(serving::retention_error(error)),
            Ok(Err(_)) => return Err(ImportError::Closed),
        };
        if batch.blocks().len() != 1 {
            return Ok(false);
        }
        match importer.retain_custody_context(batch.blocks()[0].clone()).await {
            Ok(()) => Ok(true),
            Err(
                ImportError::Backpressure |
                ImportError::Bounds |
                ImportError::GossipIgnore(_) |
                ImportError::GossipReject(_),
            ) => Ok(false),
            Err(error) => Err(error),
        }
    }
}
