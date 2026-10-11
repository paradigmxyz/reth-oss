//! Bounded, candidate-local accumulation of independently verified sampling columns.
//!
//! Peers may serve only their custody subset. Each original response is proved and its transport
//! lease released before another request. Bounded candidate originals survive retries and are
//! freshly proved at every attempt; checked indices survive only within that attempt.

use super::*;
use reth_embedded_beacon_network::availability_rpc::{SidecarKind, WireSidecar};
use std::collections::BTreeSet;

const MAX_NO_PROGRESS: usize = 16;
const MAX_SAMPLES: usize = 8;
const MAX_REQUESTS: usize = MAX_NO_PROGRESS * MAX_SAMPLES;
const ATTEMPT_TIMEOUT: Duration = Duration::from_secs(60);

impl RetrievalAvailability {
    pub(super) async fn verify_columns(
        &self,
        candidate: Candidate,
        block: BeaconBlock,
    ) -> Result<(), RetrievalError> {
        let started = Instant::now();
        let required = self.sampling.columns()?;
        let verifier = Arc::new(ColumnVerifier { config: self.config.clone(), candidate, block });
        if verifier.no_data()? {
            // Empty commitments still pass the native candidate-bound no-data check.
            verifier.verify(&[], &required)?;
            return Ok(());
        }
        let mut progress = ColumnProgress::new(required)?;
        progress.logging = Some(self.staging.clone());
        let retained = self.prove_retained_columns(verifier.clone(), progress.remaining()).await?;
        progress.record(&retained)?;
        let mut no_progress = 0;
        for _ in 0..MAX_REQUESTS {
            if progress.check_completion(candidate.root, started)? {
                return Ok(());
            }
            let remaining = progress.remaining();
            let requested_columns = remaining.len();
            let request = SidecarRequest::ColumnRoots(vec![ColumnIdentifier {
                block_root: candidate.root,
                columns: remaining.clone(),
            }]);
            let checked = match self.request(request).await {
                Ok(batch) => {
                    // Finish the exact response's proof even if its elapsed budget crosses
                    // the deadline. Never cancel ownership or discard a complete valid sample set.
                    let verifier = verifier.clone();
                    self.prove(batch, move |batch| verifier.verify(batch.sidecars(), &remaining))
                        .await?
                }
                Err(RetrievalError::Missing) => Vec::new(),
                Err(error) => return Err(error),
            };
            progress.record(&checked)?;
            tracing::debug!(target: "beacon::availability",
                root = %alloy_primitives::B256::from(candidate.root),
                requested_columns, verified_columns = checked.len(),
                remaining_columns = progress.remaining().len(),
                "[beacon] Verified original partial sampling response");
            // Completion wins over deadline exhaustion only after every original proof passed.
            if progress.check_completion(candidate.root, started)? {
                return Ok(());
            }
            if checked.is_empty() {
                no_progress += 1;
                if no_progress >= MAX_NO_PROGRESS {
                    return Err(progress.missing(candidate.root, started, "peer_sweep_exhausted"));
                }
                // A broker without eligible peers may complete Missing immediately.
                tokio::task::yield_now().await;
            } else {
                no_progress = 0;
            }
        }
        Err(progress.missing(candidate.root, started, "request_budget"))
    }

    async fn prove_retained_columns(
        &self,
        verifier: Arc<ColumnVerifier>,
        required: Vec<u64>,
    ) -> Result<Vec<u64>, RetrievalError> {
        let availability = self.clone();
        let permit =
            self.proofs.clone().try_acquire_owned().map_err(|_| RetrievalError::Capacity)?;
        tokio::task::spawn_blocking(move || {
            // Neither candidate staging nor stored checksums are validity receipts. Re-prove
            // the original bytes on the reserved worker without creating a transport batch.
            let _permit = permit;
            let mut originals = availability.staged_columns(verifier.candidate, &required)?;
            let mut present = originals.iter().map(WireSidecar::index).collect::<BTreeSet<_>>();
            if let Some(archive) = &availability.archive {
                let Some((_, retained)) = archive.selection()? else {
                    return Err(RetrievalError::Configuration);
                };
                for index in required.iter().copied().filter(|index| retained.contains(index)) {
                    if !present.contains(&index) &&
                        let Some(wire) = archive.read(
                            verifier.candidate.root,
                            verifier.candidate.slot,
                            index,
                            true,
                        )?
                    {
                        present.insert(index);
                        originals.push(wire);
                    }
                }
            }
            // Re-prove every retained original together. The same verifier checks all
            // signatures, headers, commitments and KZG proofs, while candidate hashing
            // and batch setup run once rather than once for each retained sample.
            verifier.verify(&originals, &required)
        })
        .await
        .map_err(|_| RetrievalError::Closed)?
    }
}

#[derive(Debug)]
struct ColumnVerifier {
    config: Arc<ChainConfig>,
    candidate: Candidate,
    block: BeaconBlock,
}

impl ColumnVerifier {
    fn no_data(&self) -> Result<bool, RetrievalError> {
        match &self.block {
            BeaconBlock::Fulu(block, _) => Ok(block.body.blob_kzg_commitments.values.is_empty()),
            BeaconBlock::Gloas(block, _) => Ok(block
                .body
                .signed_execution_payload_bid
                .message
                .blob_kzg_commitments
                .values
                .is_empty()),
            _ => Err(RetrievalError::Configuration),
        }
    }

    fn verify(&self, wires: &[WireSidecar], allowed: &[u64]) -> Result<Vec<u64>, RetrievalError> {
        let preset = preset(&self.config);
        let fork = self.block.schema_fork();
        let context = self
            .config
            .fork_digest(self.candidate.slot / self.config.slots_per_epoch)
            .map_err(|_| RetrievalError::Configuration)?;
        if allowed.is_empty() || allowed.len() > MAX_SAMPLES || wires.len() > allowed.len() {
            return Err(RetrievalError::Association);
        }
        let mut seen = BTreeSet::new();
        for wire in wires {
            if wire.kind() != SidecarKind::Column ||
                wire.block_root() != self.candidate.root ||
                wire.slot() != self.candidate.slot ||
                wire.fork() != fork ||
                wire.context() != context ||
                !allowed.contains(&wire.index()) ||
                !seen.insert(wire.index())
            {
                return Err(RetrievalError::Association);
            }
        }
        let checked = seen.into_iter().collect::<Vec<_>>();
        if checked.is_empty() && !self.no_data()? {
            // An empty response establishes no sample; the caller retains its missing set.
            return Ok(checked);
        }
        let required = if checked.is_empty() { allowed } else { &checked };
        match &self.block {
            BeaconBlock::Fulu(block, _) => {
                let sidecars = wires
                    .iter()
                    .map(|wire| {
                        let sidecar = fulu::DataColumnSidecar::from_ssz(wire.ssz(), preset)
                            .map_err(|_| RetrievalError::Encoding)?;
                        if Some(sidecar.signed_block_header.signature) != self.candidate.signature {
                            return Err(RetrievalError::Association);
                        }
                        Ok(sidecar)
                    })
                    .collect::<Result<Vec<_>, RetrievalError>>()?;
                let maximum = self
                    .config
                    .blob_parameters(block.slot / self.config.slots_per_epoch)
                    .map_err(|_| RetrievalError::Configuration)?
                    .max_blobs_per_block as usize;
                verify_fulu_sampling_batch(
                    ethereum_kzg_settings(0),
                    self.candidate.root,
                    block,
                    &sidecars,
                    preset,
                    maximum,
                    required,
                )
                .map_err(|_| RetrievalError::Proof)?;
            }
            BeaconBlock::Gloas(block, _) => {
                let sidecars = wires
                    .iter()
                    .map(|wire| {
                        gloas::DataColumnSidecar::from_ssz(wire.ssz(), preset)
                            .map_err(|_| RetrievalError::Encoding)
                    })
                    .collect::<Result<Vec<_>, RetrievalError>>()?;
                verify_gloas_sampling_batch(
                    ethereum_kzg_settings(0),
                    self.candidate.root,
                    block,
                    &sidecars,
                    preset,
                    required,
                )
                .map_err(|_| RetrievalError::Proof)?;
            }
            _ => return Err(RetrievalError::Configuration),
        }
        Ok(checked)
    }
}

#[derive(Debug)]
struct ColumnProgress {
    required: BTreeSet<u64>,
    checked: BTreeSet<u64>,
    logging: Option<Arc<Mutex<staging::ProofInputs>>>,
}

impl ColumnProgress {
    fn new(required: Vec<u64>) -> Result<Self, RetrievalError> {
        let count = required.len();
        let required = required.into_iter().collect::<BTreeSet<_>>();
        if count != MAX_SAMPLES ||
            required.len() != count ||
            required.iter().any(|index| *index >= 128)
        {
            return Err(RetrievalError::Configuration);
        }
        Ok(Self { required, checked: BTreeSet::new(), logging: None })
    }

    fn check_completion(&self, root: Root, started: Instant) -> Result<bool, RetrievalError> {
        if self.checked.len() == self.required.len() {
            return Ok(true);
        }
        if started.elapsed() >= ATTEMPT_TIMEOUT {
            return Err(self.missing(root, started, "attempt_deadline"));
        }
        Ok(false)
    }

    fn missing(&self, root: Root, started: Instant, reason: &'static str) -> RetrievalError {
        let info = self.logging.as_ref().is_some_and(|stage| {
            stage.lock().ok().is_some_and(|mut stage| stage.sampling_info_due(Instant::now()))
        });
        if info {
            tracing::info!(target: "beacon::availability", root = %alloy_primitives::B256::from(root),
                required_columns = self.required.len(), verified_columns = self.checked.len(),
                remaining_columns = self.required.len().saturating_sub(self.checked.len()),
                elapsed_secs = started.elapsed().as_secs(), reason,
                "[beacon] Waiting for required original sampling columns");
        } else {
            tracing::debug!(target: "beacon::availability", root = %alloy_primitives::B256::from(root),
                required_columns = self.required.len(), verified_columns = self.checked.len(),
                remaining_columns = self.required.len().saturating_sub(self.checked.len()),
                elapsed_secs = started.elapsed().as_secs(), reason,
                "[beacon] Waiting for required original sampling columns");
        }
        RetrievalError::Missing
    }
    fn remaining(&self) -> Vec<u64> {
        self.required.difference(&self.checked).copied().collect()
    }

    fn record(&mut self, indices: &[u64]) -> Result<(), RetrievalError> {
        let count = indices.len();
        let indices = indices.iter().copied().collect::<BTreeSet<_>>();
        if indices.len() != count ||
            !indices.is_subset(&self.required) ||
            !indices.is_disjoint(&self.checked)
        {
            return Err(RetrievalError::Association);
        }
        self.checked.extend(indices);
        Ok(())
    }
}

#[cfg(test)]
mod tests;
