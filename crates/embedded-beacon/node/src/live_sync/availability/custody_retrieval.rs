//! One bounded custody request keeps the peer broker live and respects active import ownership.

use super::*;
use crate::import_coordinator::{ImportCoordinator, ImportError, UnsignedCustodyAnchor};
use reth_embedded_beacon_state_transition::ssz::SignedBeaconBlock;

struct CustodyRequestGuard {
    candidate: Option<CandidateGuard>,
    active: Arc<AtomicBool>,
}
impl Drop for CustodyRequestGuard {
    fn drop(&mut self) {
        drop(self.candidate.take());
        self.active.store(false, Ordering::Release);
    }
}

impl RetrievalAvailability {
    pub(super) async fn acquire_anchor_columns(
        &self,
        importer: &ImportCoordinator,
        anchor: UnsignedCustodyAnchor,
        columns: [u64; 4],
    ) -> Result<bool, ImportError> {
        if anchor.config_digest() != self.digest {
            return Err(ImportError::Configuration)
        }
        if self
            .custody_retrieving
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Ok(false)
        }
        let mut guard =
            CustodyRequestGuard { candidate: None, active: self.custody_retrieving.clone() };
        {
            let mut candidate = self.candidate.lock().map_err(|_| ImportError::Closed)?;
            if candidate.is_some() {
                return Ok(false)
            }
            *candidate =
                Some(Candidate { root: anchor.root(), slot: anchor.slot(), signature: None });
            guard.candidate = Some(CandidateGuard {
                candidate: self.candidate.clone(),
                staging: self.staging.clone(),
            });
        }
        let request = SidecarRequest::ColumnRoots(vec![ColumnIdentifier {
            block_root: anchor.root(),
            columns: columns.to_vec(),
        }]);
        let batch = match self.request(request).await {
            Ok(batch) => batch,
            Err(
                RetrievalError::Capacity |
                RetrievalError::Timeout |
                RetrievalError::Missing |
                RetrievalError::Association |
                RetrievalError::Encoding |
                RetrievalError::Proof,
            ) => return Ok(false),
            Err(error) => return Err(serving::retention_error(error)),
        };
        if batch.sidecars().is_empty() {
            return Ok(false)
        }
        match importer.retain_anchor_columns(anchor.root(), batch.sidecars().to_vec()).await {
            Ok(()) => Ok(batch.sidecars().len() == 4),
            Err(
                ImportError::Backpressure |
                ImportError::Bounds |
                ImportError::GossipIgnore(_) |
                ImportError::GossipReject(_),
            ) => Ok(false),
            Err(error) => Err(error),
        }
    }
    pub(super) async fn acquire_signed_columns(
        &self,
        importer: &ImportCoordinator,
        wire: WireBlock,
        columns: [u64; 4],
    ) -> Result<bool, ImportError> {
        if self
            .custody_retrieving
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Ok(false)
        }
        let mut guard =
            CustodyRequestGuard { candidate: None, active: self.custody_retrieving.clone() };
        match self.bind_candidate(&wire) {
            Ok(candidate) => guard.candidate = Some(candidate),
            Err(RetrievalError::Capacity) => return Ok(false),
            Err(error) => return Err(serving::retention_error(error)),
        }
        let Some(archive) = self.archive.clone() else { return Ok(false) };
        let Ok(permit) = self.proofs.clone().try_acquire_owned() else { return Ok(false) };
        let original = wire.clone();
        let config = self.config.clone();
        let request = tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let signed =
                SignedBeaconBlock::from_ssz(original.ssz(), original.fork(), preset(&config))
                    .map_err(|_| RetrievalError::Encoding)?;
            let (indices, column) = coverage::required_sidecars(&signed, columns);
            let mut missing = Vec::new();
            for index in indices {
                if archive.read(original.root(), original.slot(), index, column)?.is_none() {
                    missing.push(index);
                }
                if missing.len() == 4 {
                    break
                }
            }
            if missing.is_empty() {
                return Ok(None)
            }
            let expected = missing.len();
            let request = if column {
                SidecarRequest::ColumnRoots(vec![ColumnIdentifier {
                    block_root: original.root(),
                    columns: missing,
                }])
            } else {
                SidecarRequest::BlobRoots(
                    missing
                        .into_iter()
                        .map(|index| BlobIdentifier { block_root: original.root(), index })
                        .collect(),
                )
            };
            Ok::<_, RetrievalError>(Some((request, expected)))
        })
        .await
        .map_err(|_| ImportError::Closed)?
        .map_err(serving::retention_error)?;
        let Some((request, expected)) = request else { return Ok(false) };
        let batch = match self.request(request).await {
            Ok(batch) => batch,
            Err(
                RetrievalError::Capacity |
                RetrievalError::Timeout |
                RetrievalError::Missing |
                RetrievalError::Association |
                RetrievalError::Encoding |
                RetrievalError::Proof,
            ) => return Ok(false),
            Err(error) => return Err(serving::retention_error(error)),
        };
        if batch.sidecars().len() != expected {
            return Ok(false)
        }
        // The dedicated native owner freshly verifies this entire original batch before any
        // private seal or durable pin. Do not repeat synchronous KZG work on the executor.
        match importer.retain_anchor_columns(wire.root(), batch.sidecars().to_vec()).await {
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
    pub(crate) fn custody_retrieving(&self) -> bool {
        self.custody_retrieving.load(Ordering::Acquire)
    }
}
