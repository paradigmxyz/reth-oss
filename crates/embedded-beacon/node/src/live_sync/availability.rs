//! Exact candidate-bound retrieval and mandatory mathematical availability verification.
//! The broker owns at most one outstanding request and retains transport response leases until
//! its bounded blocking proof job finishes. Original-header signatures must match the original
//! candidate signature, which the strict importer independently verifies. No wire success grants
//! consensus or Engine authority. Identity-selected samples are checked, without asserting custody.

use crate::bootstrap::checked_config_digest;
use c_kzg::ethereum_kzg_settings;
use reth_embedded_beacon_config::{ChainConfig, Preset as ConfigPreset};
use reth_embedded_beacon_data_availability::admission::{
    verify_blob_batch, verify_fulu_sampling_batch, verify_gloas_sampling_batch,
};
use reth_embedded_beacon_fork_choice::{
    compact::{AdmittedState, VerifiedEnvelope},
    data_availability::DataAvailability as ClassicAvailability,
    gloas::DataAvailability as GloasAvailability,
};
use reth_embedded_beacon_network::{
    availability_rpc::{BlobIdentifier, ColumnIdentifier, SidecarBatch, SidecarRequest},
    block_rpc::WireBlock,
    envelope_rpc::{EnvelopeBatch, EnvelopeBlockReference, EnvelopeRequest},
};
use reth_embedded_beacon_primitives::{hash_pair, Fork, Root};
use reth_embedded_beacon_state_transition::ssz::{
    auxiliary::{deneb, electra, fulu, gloas},
    gloas::BeaconBlock as GloasBlock,
    phase0::SignedBeaconBlockHeader,
    BeaconBlock, Preset,
};
use std::{
    future::Future,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, oneshot, OwnedSemaphorePermit, Semaphore};

pub(super) const RETRIEVAL_TIMEOUT: Duration = Duration::from_secs(20);

/// Actual availability callback shared by the dedicated importer and its peer event owner.
#[derive(Clone, Debug)]
pub struct RetrievalAvailability {
    config: Arc<ChainConfig>,
    digest: Root,
    sender: mpsc::Sender<RetrievalJob>,
    envelope_sender: mpsc::Sender<EnvelopeRetrievalJob>,
    requests: Arc<Semaphore>,
    proofs: Arc<Semaphore>,
    candidate: Arc<Mutex<Option<Candidate>>>,
    halted: Arc<AtomicBool>,
    deferred_data: Arc<AtomicBool>,
    archive: Option<archive::SidecarArchive>,
    coverage: Arc<Mutex<coverage::CustodyCoverage>>,
    staging: Arc<Mutex<staging::ProofInputs>>,
    custody_retrieving: Arc<AtomicBool>,
    history_sender: mpsc::Sender<history_retrieval::HistoricalBlockJob>,
    history_requests: Arc<Semaphore>,
    sampling: Arc<sampling::SamplingSelection>,
}

impl RetrievalAvailability {
    /// Create a bounded broker; this does not open sockets or assert any availability.
    pub fn new(config: ChainConfig) -> Result<(Self, RetrievalBroker), RetrievalError> {
        let digest = checked_config_digest(&config).map_err(|_| RetrievalError::Configuration)?;
        let (sender, receiver) = mpsc::channel(1);
        let (envelope_sender, envelopes) = mpsc::channel(1);
        let (history_sender, history) = mpsc::channel(1);
        let requests = Arc::new(Semaphore::new(1));
        let proofs = Arc::new(Semaphore::new(1));
        let candidate = Arc::new(Mutex::new(None));
        let config = Arc::new(config);
        Ok((
            Self {
                config,
                digest,
                sender,
                envelope_sender,
                requests,
                proofs,
                candidate,
                halted: Arc::new(AtomicBool::new(false)),
                deferred_data: Arc::new(AtomicBool::new(false)),
                archive: None,
                coverage: Arc::new(Mutex::new(coverage::CustodyCoverage::default())),
                staging: Arc::new(Mutex::new(staging::ProofInputs::default())),
                custody_retrieving: Arc::new(AtomicBool::new(false)),
                history_sender,
                history_requests: Arc::new(Semaphore::new(1)),
                sampling: Arc::default(),
            },
            RetrievalBroker { receiver, envelopes, history, digest },
        ))
    }

    /// Retain proof-checked original sidecars using the exact existing shared writer.
    /// Broader writer identity is captured independently of the consensus configuration digest.
    pub fn with_shared_store(
        config: ChainConfig,
        shared: reth_embedded_beacon_shared_data::SharedDataStore,
    ) -> Result<(Self, RetrievalBroker), RetrievalError> {
        let (mut availability, broker) = Self::new(config)?;
        availability.archive = Some(archive::SidecarArchive::new(
            availability.config.clone(),
            availability.digest,
            shared,
        )?);
        Ok((availability, broker))
    }

    pub(super) fn bind_peer_identity(&self, node_id: Root) -> Result<(), RetrievalError> {
        if let Some(archive) = &self.archive {
            archive.bind_identity(node_id)?;
        }
        // Publish sampling readiness after the archive sees the same persisted identity.
        self.sampling.bind(node_id)
    }

    #[cfg(test)]
    pub(crate) fn bind_custody_peer_for_tests(
        &self,
        peer: &crate::peer_runtime::CheckpointPeerRuntime,
    ) -> Result<(), RetrievalError> {
        let node_id = peer.local_enr().map_err(|_| RetrievalError::Configuration)?.node_id().raw();
        self.bind_peer_identity(node_id)
    }

    #[cfg(test)]
    pub(crate) fn prune_custody_clock_for_tests(
        &self,
        status: &crate::import_coordinator::AuthenticatedStatus,
    ) -> Result<(), RetrievalError> {
        let Some(archive) = &self.archive else { return Err(RetrievalError::Missing) };
        archive.prune_custody_clock_for_tests(status)
    }

    pub(crate) fn bind_candidate(
        &self,
        wire: &WireBlock,
    ) -> Result<CandidateGuard, RetrievalError> {
        if self.config.fork_at_slot(wire.slot()).map_err(|_| RetrievalError::Configuration)?.fork !=
            wire.fork() ||
            self.config
                .fork_digest(wire.slot() / self.config.slots_per_epoch)
                .map_err(|_| RetrievalError::Configuration)? !=
                wire.context()
        {
            return Err(RetrievalError::Association);
        }
        // WireBlock construction has already completely decoded the canonical signed container.
        let signature = wire
            .ssz()
            .get(4..100)
            .ok_or(RetrievalError::Association)?
            .try_into()
            .map_err(|_| RetrievalError::Association)?;
        let mut candidate = self.candidate.lock().map_err(|_| RetrievalError::Closed)?;
        if candidate.is_some() {
            return Err(RetrievalError::Capacity);
        }
        self.deferred_data.store(false, Ordering::Release);
        *candidate =
            Some(Candidate { root: wire.root(), slot: wire.slot(), signature: Some(signature) });
        Ok(CandidateGuard { candidate: self.candidate.clone(), staging: self.staging.clone() })
    }

    /// Bind exact Gloas column retrieval to a genuinely retained native block.
    /// Gloas columns carry no proposer-header signature; historical blob/header paths still
    /// require the original signed-candidate signature and cannot use this binding.
    pub(crate) fn bind_retained_block(
        &self,
        block: &BeaconBlock,
    ) -> Result<CandidateGuard, RetrievalError> {
        let reference = EnvelopeBlockReference::from_block(block, &self.config)
            .map_err(|_| RetrievalError::Association)?;
        let mut candidate = self.candidate.lock().map_err(|_| RetrievalError::Closed)?;
        if candidate.is_some() {
            return Err(RetrievalError::Capacity);
        }
        self.deferred_data.store(false, Ordering::Release);
        *candidate =
            Some(Candidate { root: reference.root(), slot: reference.slot(), signature: None });
        Ok(CandidateGuard { candidate: self.candidate.clone(), staging: self.staging.clone() })
    }

    /// Bind Gloas-only column retrieval to exact native block metadata returned by the owner.
    /// The proof callback independently matches this root and slot to its actual graph block.
    pub(crate) fn bind_envelope_reference(
        &self,
        reference: &EnvelopeBlockReference,
    ) -> Result<CandidateGuard, RetrievalError> {
        if self
            .config
            .fork_at_slot(reference.slot())
            .map_err(|_| RetrievalError::Configuration)?
            .fork !=
            Fork::Gloas ||
            self.config
                .fork_digest(reference.slot() / self.config.slots_per_epoch)
                .map_err(|_| RetrievalError::Configuration)? !=
                reference.context()
        {
            return Err(RetrievalError::Association);
        }
        let mut candidate = self.candidate.lock().map_err(|_| RetrievalError::Closed)?;
        if candidate.is_some() {
            return Err(RetrievalError::Capacity);
        }
        self.deferred_data.store(false, Ordering::Release);
        *candidate =
            Some(Candidate { root: reference.root(), slot: reference.slot(), signature: None });
        Ok(CandidateGuard { candidate: self.candidate.clone(), staging: self.staging.clone() })
    }

    /// Match broker work to the retained original candidate, including the fork's sidecar kind.
    pub(super) fn owns_retrieval_job(&self, job: &RetrievalJob) -> Result<bool, RetrievalError> {
        let candidate = self.candidate.lock().map_err(|_| RetrievalError::Closed)?;
        let Some(candidate) = candidate.as_ref() else { return Ok(false) };
        if candidate.root != job.root || candidate.slot != job.slot {
            return Ok(false)
        }
        let fork =
            self.config.fork_at_slot(job.slot).map_err(|_| RetrievalError::Configuration)?.fork;
        Ok(match (&job.request, fork) {
            (SidecarRequest::BlobRoots(ids), Fork::Deneb | Fork::Electra) => {
                ids.iter().all(|id| id.block_root == candidate.root)
            }
            (SidecarRequest::ColumnRoots(ids), Fork::Fulu | Fork::Gloas) => {
                ids.iter().all(|id| id.block_root == candidate.root)
            }
            _ => false,
        })
    }
    pub(crate) const fn config_digest(&self) -> Root {
        self.digest
    }

    /// Local queue, proof worker or unresolved timeout failures require owner reconciliation.
    pub(crate) fn is_halted(&self) -> bool {
        self.halted.load(Ordering::Acquire)
    }

    pub(crate) fn deferred_data(&self) -> bool {
        self.deferred_data.load(Ordering::Acquire)
    }

    /// Consume one known missing-data result from the exclusively bound replay candidate.
    pub(crate) fn take_deferred_data(&self) -> bool {
        self.deferred_data.swap(false, Ordering::AcqRel)
    }

    fn check_health<T>(&self, result: &Result<T, RetrievalError>) {
        if matches!(result, Err(RetrievalError::Missing)) {
            self.deferred_data.store(true, Ordering::Release);
        }
        if matches!(
            result,
            Err(RetrievalError::Configuration |
                RetrievalError::Capacity |
                RetrievalError::Closed |
                RetrievalError::Timeout |
                RetrievalError::Storage)
        ) {
            self.halted.store(true, Ordering::Release);
        }
    }

    fn request(
        &self,
        request: SidecarRequest,
    ) -> impl Future<Output = Result<SidecarBatch, RetrievalError>> + Send + 'static {
        let admitted = self.try_request(request);
        async move {
            let reply = admitted?;
            tokio::time::timeout(RETRIEVAL_TIMEOUT, reply)
                .await
                .map_err(|_| RetrievalError::Timeout)?
                .map_err(|_| RetrievalError::Closed)?
        }
    }

    fn try_request(
        &self,
        request: SidecarRequest,
    ) -> Result<oneshot::Receiver<Result<SidecarBatch, RetrievalError>>, RetrievalError> {
        request.validate_outbound().map_err(|_| RetrievalError::Capacity)?;
        let permit =
            self.requests.clone().try_acquire_owned().map_err(|_| RetrievalError::Capacity)?;
        let queue = self.sender.try_reserve().map_err(|_| RetrievalError::Capacity)?;
        let (reply, receiver) = oneshot::channel();
        let candidate = self
            .candidate
            .lock()
            .map_err(|_| RetrievalError::Closed)?
            .as_ref()
            .copied()
            .ok_or(RetrievalError::Association)?;
        let exact = match &request {
            SidecarRequest::BlobRoots(ids) => ids.iter().all(|id| id.block_root == candidate.root),
            SidecarRequest::ColumnRoots(ids) => {
                ids.iter().all(|id| id.block_root == candidate.root)
            }
            _ => false,
        };
        if !exact {
            return Err(RetrievalError::Association);
        }
        queue.send(RetrievalJob {
            request,
            root: candidate.root,
            slot: candidate.slot,
            reply,
            created: Instant::now(),
            _permit: permit,
        });
        Ok(receiver)
    }

    /// Retrieve one original signed envelope for the actual active, already decoded candidate.
    /// The returned transport lease remains owned until mandatory envelope import finishes.
    pub(crate) async fn request_envelope(
        &self,
        wire: &WireBlock,
    ) -> Result<EnvelopeBatch, RetrievalError> {
        let result = async {
            if wire.fork() != Fork::Gloas {
                return Err(RetrievalError::Association);
            }
            self.candidate(wire.root(), wire.slot())?;
            let request = EnvelopeRequest::Roots(vec![wire.root()]);
            request.validate_outbound().map_err(|_| RetrievalError::Association)?;
            let permit =
                self.requests.clone().try_acquire_owned().map_err(|_| RetrievalError::Capacity)?;
            let queue = self.envelope_sender.try_reserve().map_err(|_| RetrievalError::Capacity)?;
            let (reply, receiver) = oneshot::channel();
            queue.send(EnvelopeRetrievalJob {
                metadata: EnvelopeBlockReference::from(wire),
                reply,
                created: Instant::now(),
                _permit: permit,
            });
            let batch = tokio::time::timeout(RETRIEVAL_TIMEOUT, receiver)
                .await
                .map_err(|_| RetrievalError::Timeout)?
                .map_err(|_| RetrievalError::Closed)??;
            let metadata = EnvelopeBlockReference::from(wire);
            request
                .matches(&self.config, &batch, |root| (root == wire.root()).then_some(&metadata))
                .map_err(|_| RetrievalError::Association)?;
            if batch.envelopes().len() != 1 {
                return Err(RetrievalError::Missing);
            }
            Ok(batch)
        }
        .await;
        self.check_health(&result);
        result
    }

    fn candidate(&self, root: Root, slot: u64) -> Result<Candidate, RetrievalError> {
        self.candidate
            .lock()
            .map_err(|_| RetrievalError::Closed)?
            .as_ref()
            .filter(|candidate| candidate.root == root && candidate.slot == slot)
            .copied()
            .ok_or(RetrievalError::Association)
    }

    async fn prove<T, F>(&self, batch: SidecarBatch, verify: F) -> Result<T, RetrievalError>
    where
        T: Send + 'static,
        F: FnOnce(SidecarBatch) -> Result<T, RetrievalError> + Send + 'static,
    {
        let permit =
            self.proofs.clone().try_acquire_owned().map_err(|_| RetrievalError::Capacity)?;
        let availability = self.clone();
        tokio::task::spawn_blocking(move || {
            // Cancellation cannot free the worker or transport reservations while work continues.
            let _permit = permit;
            let retained = batch.clone();
            let result = verify(batch)?;
            availability.stage_verified(&retained)?;
            Ok(result)
        })
        .await
        .map_err(|_| RetrievalError::Closed)?
    }

    async fn verify_classic(&self, root: Root, block: &BeaconBlock) -> Result<(), RetrievalError> {
        if block.hash_tree_root().map_err(|_| RetrievalError::Association)? != root {
            return Err(RetrievalError::Association);
        }
        let header = block.header().map_err(|_| RetrievalError::Association)?;
        let candidate = self.candidate(root, header.slot)?;
        let preset = preset(&self.config);
        match block {
            BeaconBlock::Deneb(value, _) => {
                let commitments = value.body.blob_kzg_commitments.values.clone();
                self.verify_blobs(candidate, commitments, Fork::Deneb, preset).await
            }
            BeaconBlock::Electra(value, _) => {
                let commitments = value.body.blob_kzg_commitments.values.clone();
                self.verify_blobs(candidate, commitments, Fork::Electra, preset).await
            }
            BeaconBlock::Fulu(value, _) => {
                self.verify_columns(candidate, BeaconBlock::Fulu(value.clone(), preset)).await
            }
            _ => Err(RetrievalError::Configuration),
        }
    }

    async fn verify_blobs(
        &self,
        candidate: Candidate,
        commitments: Vec<[u8; 48]>,
        fork: Fork,
        preset: Preset,
    ) -> Result<(), RetrievalError> {
        if commitments.is_empty() {
            return verify_blob_batch(ethereum_kzg_settings(0), &[], &[], &[])
                .map_err(|_| RetrievalError::Proof);
        }
        let request = SidecarRequest::BlobRoots(
            (0..commitments.len())
                .map(|index| BlobIdentifier { block_root: candidate.root, index: index as u64 })
                .collect(),
        );
        let batch = self.request(request).await?;
        self.prove(batch, move |batch| {
            let mut ordered = vec![None; commitments.len()];
            for wire in batch.sidecars() {
                if wire.block_root() != candidate.root ||
                    wire.slot() != candidate.slot ||
                    wire.fork() != fork
                {
                    return Err(RetrievalError::Association);
                }
                let (index, blob, commitment, proof, signed_header, inclusion) = match fork {
                    Fork::Deneb => {
                        let s = deneb::BlobSidecar::from_ssz(wire.ssz(), preset)
                            .map_err(|_| RetrievalError::Encoding)?;
                        (
                            s.index,
                            s.blob,
                            s.kzg_commitment,
                            s.kzg_proof,
                            s.signed_block_header,
                            s.kzg_commitment_inclusion_proof.values,
                        )
                    }
                    Fork::Electra => {
                        let s = electra::BlobSidecar::from_ssz(wire.ssz(), preset)
                            .map_err(|_| RetrievalError::Encoding)?;
                        (
                            s.index,
                            s.blob,
                            s.kzg_commitment,
                            s.kzg_proof,
                            s.signed_block_header,
                            s.kzg_commitment_inclusion_proof.values,
                        )
                    }
                    _ => return Err(RetrievalError::Configuration),
                };
                let index = usize::try_from(index).map_err(|_| RetrievalError::Association)?;
                if commitments.get(index) != Some(&commitment) ||
                    Some(signed_header.signature) != candidate.signature ||
                    signed_header.message.hash_tree_root() != candidate.root
                {
                    return Err(RetrievalError::Association);
                }
                verify_blob_inclusion(index as u64, commitment, &inclusion, &signed_header)?;
                let slot = ordered.get_mut(index).ok_or(RetrievalError::Association)?;
                if slot.is_some() {
                    return Err(RetrievalError::Missing);
                }
                *slot = Some((blob, proof));
            }
            let (blobs, proofs): (Vec<_>, Vec<_>) = ordered
                .into_iter()
                .collect::<Option<Vec<_>>>()
                .ok_or(RetrievalError::Missing)?
                .into_iter()
                .unzip();
            verify_blob_batch(ethereum_kzg_settings(0), &commitments, &blobs, &proofs)
                .map_err(|_| RetrievalError::Proof)
        })
        .await
    }

    async fn verify_gloas(&self, root: Root, block: &GloasBlock) -> Result<(), RetrievalError> {
        let preset = preset(&self.config);
        if block.hash_tree_root(preset).map_err(|_| RetrievalError::Association)? != root {
            return Err(RetrievalError::Association);
        }
        let candidate = self.candidate(root, block.slot)?;
        self.verify_columns(candidate, BeaconBlock::Gloas(block.clone(), preset)).await
    }
}

impl ClassicAvailability for RetrievalAvailability {
    type Error = RetrievalError;
    fn retain_admitted(&self, admitted: &AdmittedState<'_>) -> Result<(), Self::Error> {
        self.retain_native_block(admitted)
    }
    async fn verify(&self, root: Root, block: &BeaconBlock) -> Result<(), Self::Error> {
        let result = self.verify_classic(root, block).await;
        self.check_health(&result);
        result
    }
}
impl GloasAvailability for RetrievalAvailability {
    type Error = RetrievalError;
    fn retain_verified_envelope(&self, verified: &VerifiedEnvelope<'_>) -> Result<(), Self::Error> {
        self.retain_native_envelope(verified)
    }
    async fn verify(&self, root: Root, block: &GloasBlock) -> Result<(), Self::Error> {
        let result = self.verify_gloas(root, block).await;
        self.check_health(&result);
        result
    }
}

/// The single receiver belongs exclusively to the peer event owner.
#[derive(Debug)]
pub struct RetrievalBroker {
    pub(super) receiver: mpsc::Receiver<RetrievalJob>,
    pub(super) envelopes: mpsc::Receiver<EnvelopeRetrievalJob>,
    pub(super) history: mpsc::Receiver<history_retrieval::HistoricalBlockJob>,
    pub(super) digest: Root,
}
#[derive(Debug)]
pub(super) struct RetrievalJob {
    pub(super) request: SidecarRequest,
    pub(super) root: Root,
    pub(super) slot: u64,
    pub(super) reply: oneshot::Sender<Result<SidecarBatch, RetrievalError>>,
    pub(super) created: Instant,
    _permit: OwnedSemaphorePermit,
}
impl RetrievalJob {
    pub(super) fn complete(self, result: Result<SidecarBatch, RetrievalError>) {
        let Self { reply, _permit, .. } = self;
        // Release admission before waking an owner that may immediately request its next group.
        drop(_permit);
        let _ = reply.send(result);
    }
}
#[derive(Debug)]
pub(super) struct EnvelopeRetrievalJob {
    pub(super) metadata: EnvelopeBlockReference,
    pub(super) reply: oneshot::Sender<Result<EnvelopeBatch, RetrievalError>>,
    pub(super) created: Instant,
    _permit: OwnedSemaphorePermit,
}
impl EnvelopeRetrievalJob {
    pub(super) fn complete(self, result: Result<EnvelopeBatch, RetrievalError>) {
        let Self { reply, _permit, .. } = self;
        drop(_permit);
        let _ = reply.send(result);
    }
}
#[derive(Clone, Copy, Debug)]
struct Candidate {
    root: Root,
    slot: u64,
    signature: Option<[u8; 96]>,
}
pub(crate) struct CandidateGuard {
    candidate: Arc<Mutex<Option<Candidate>>>,
    staging: Arc<Mutex<staging::ProofInputs>>,
}
impl Drop for CandidateGuard {
    fn drop(&mut self) {
        if let Ok(mut candidate) = self.candidate.lock() {
            *candidate = None;
            if let Ok(mut staged) = self.staging.lock() {
                staged.clear()
            }
        }
    }
}

/// Missing and malformed remote data remain distinct from local broker failures.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RetrievalError {
    /// Network or sampling policy is not the concretely configured profile.
    #[error("availability retrieval configuration mismatch")]
    Configuration,
    /// Immediate bounded admission failed; no uncounted request waiter is retained.
    #[error("availability broker saturated")]
    Capacity,
    /// The actual broker or proof worker is unavailable.
    #[error("availability broker closed")]
    Closed,
    /// The actual request deadline elapsed; no availability is granted.
    #[error("availability retrieval timed out")]
    Timeout,
    /// An expected original item was absent or duplicated.
    #[error("required original availability item missing")]
    Missing,
    /// Original context, header, signature, root or commitment does not match the candidate.
    #[error("availability candidate association mismatch")]
    Association,
    /// Complete SSZ decoding failed.
    #[error("availability original SSZ malformed")]
    Encoding,
    /// Actual inclusion or KZG proof failed.
    #[error("availability proof rejected")]
    Proof,
    /// Shared data or proof-input publication is corrupt or uncertain; never remote invalidity.
    #[error("availability shared retention failed")]
    Storage,
}

const fn preset(config: &ChainConfig) -> Preset {
    match config.preset {
        ConfigPreset::Mainnet => Preset::Mainnet,
        ConfigPreset::Minimal => Preset::Minimal,
    }
}
fn verify_blob_inclusion(
    index: u64,
    commitment: [u8; 48],
    branch: &[Root],
    header: &SignedBeaconBlockHeader,
) -> Result<(), RetrievalError> {
    if index >= 4096 || branch.len() != 17 {
        return Err(RetrievalError::Association);
    }
    let mut first = [0; 32];
    first.copy_from_slice(&commitment[..32]);
    let mut second = [0; 32];
    second[..16].copy_from_slice(&commitment[32..]);
    let mut root = hash_pair(first, second);
    let mut position = (11u64 << 13) | index;
    for sibling in branch {
        root =
            if position & 1 == 0 { hash_pair(root, *sibling) } else { hash_pair(*sibling, root) };
        position >>= 1;
    }
    if root == header.message.body_root {
        Ok(())
    } else {
        Err(RetrievalError::Proof)
    }
}

#[cfg(test)]
mod tests;

mod archive;

pub(crate) mod coverage;
mod custody_retrieval;
pub(super) mod history_retrieval;
pub(crate) mod serving;

mod staging;

mod columns;
mod gossip;
mod observation;
mod sampling;

pub use observation::AvailabilityObservation;
