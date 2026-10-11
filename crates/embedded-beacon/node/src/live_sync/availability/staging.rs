//! Candidate-local proof inputs, committed only through an actual sealed native admission.
//!
//! Mathematical DA verification precedes signed state validation. Its temporary originals
//! therefore own a bounded allocation, never durable archive pins or valid-root caches.

use super::*;
use reth_embedded_beacon_fork_choice::compact::{
    AdmittedState, SignedBlockData, StateBinding, VerifiedEnvelope,
};
use reth_embedded_beacon_network::availability_rpc::{SidecarKind, WireSidecar};
use reth_embedded_beacon_state_transition::ssz::SignedBeaconBlock;
use std::collections::BTreeMap;

const MAX_STAGED_BYTES: usize = 32 * 1024 * 1024;
const SAMPLING_LOG_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Debug, Default)]
pub(super) struct ProofInputs {
    originals: BTreeMap<(u64, bool), WireSidecar>,
    bytes: usize,
    next_sampling_info: Option<Instant>,
}
impl ProofInputs {
    pub(super) fn clear(&mut self) {
        self.originals.clear();
        self.bytes = 0;
        self.next_sampling_info = None;
    }

    /// Rate-limit diagnostics across retries of this candidate, independently of proof state.
    pub(super) fn sampling_info_due(&mut self, now: Instant) -> bool {
        if self.next_sampling_info.is_some_and(|next| now < next) {
            return false;
        }
        self.next_sampling_info = Some(now + SAMPLING_LOG_INTERVAL);
        true
    }
}
impl RetrievalAvailability {
    pub(super) fn stage_verified(&self, batch: &SidecarBatch) -> Result<(), RetrievalError> {
        self.stage_verified_originals(batch.sidecars())
    }

    /// Retain bounded original inputs after the caller's real proof completes. A later
    /// attempt must repeat that proof; these bytes grant no availability or custody receipt.
    pub(super) fn stage_verified_originals(
        &self,
        originals: &[WireSidecar],
    ) -> Result<(), RetrievalError> {
        let columns = if originals.iter().any(|wire| wire.kind() == SidecarKind::Column) {
            self.sampling.columns()?
        } else {
            Vec::new()
        };
        let candidate = self.candidate.lock().map_err(|_| RetrievalError::Closed)?;
        let candidate = candidate.as_ref().ok_or(RetrievalError::Association)?;
        let mut staged = self.staging.lock().map_err(|_| RetrievalError::Closed)?;
        // Decide the entire batch's byte budget before changing its stage. Samples are
        // temporary candidate inputs; durable retention remains the archive's custody policy.
        let mut extra = 0usize;
        for wire in originals {
            if wire.block_root() != candidate.root || wire.slot() != candidate.slot {
                return Err(RetrievalError::Association)
            }
            let column = wire.kind() == SidecarKind::Column;
            if column && !columns.contains(&wire.index()) || !column && self.archive.is_none() {
                continue
            }
            if let Some(prior) = staged.originals.get(&(wire.index(), column)) {
                if prior != wire {
                    return Err(RetrievalError::Association)
                }
            } else {
                extra = extra.checked_add(wire.ssz().len()).ok_or(RetrievalError::Capacity)?;
            }
        }
        let total = staged.bytes.checked_add(extra).ok_or(RetrievalError::Capacity)?;
        if total > MAX_STAGED_BYTES {
            return Err(RetrievalError::Capacity)
        }
        for wire in originals {
            let column = wire.kind() == SidecarKind::Column;
            if column && columns.contains(&wire.index()) || !column && self.archive.is_some() {
                staged.originals.entry((wire.index(), column)).or_insert_with(|| wire.clone());
            }
        }
        staged.bytes = total;
        Ok(())
    }

    /// Clone only the active candidate's bounded original samples. No checked-index cache is
    /// returned: the bounded proof worker must independently verify each original again.
    pub(super) fn staged_columns(
        &self,
        expected: Candidate,
        required: &[u64],
    ) -> Result<Vec<WireSidecar>, RetrievalError> {
        let candidate = self.candidate.lock().map_err(|_| RetrievalError::Closed)?;
        let candidate = candidate.as_ref().ok_or(RetrievalError::Association)?;
        if candidate.root != expected.root ||
            candidate.slot != expected.slot ||
            candidate.signature != expected.signature
        {
            return Err(RetrievalError::Association)
        }
        let staged = self.staging.lock().map_err(|_| RetrievalError::Closed)?;
        Ok(required
            .iter()
            .filter_map(|index| staged.originals.get(&(*index, true)).cloned())
            .collect())
    }
    fn commit_binding(
        &self,
        binding: &StateBinding,
        signature: Option<[u8; 96]>,
    ) -> Result<(), RetrievalError> {
        let Some(archive) = &self.archive else { return Ok(()) };
        if self.staging.lock().map_err(|_| RetrievalError::Closed)?.originals.is_empty() {
            return Ok(())
        }
        if binding.identity().profile_digest != archive.writer_identity().config_digest ||
            binding.identity().consensus_config_digest != self.digest ||
            binding.genesis_validators_root() != self.config.genesis_validators_root ||
            binding.genesis_time() != self.config.genesis_time
        {
            return Err(RetrievalError::Configuration)
        }
        let candidate = self.candidate.lock().map_err(|_| RetrievalError::Closed)?;
        let candidate = candidate.as_ref().ok_or(RetrievalError::Association)?;
        if candidate.root != binding.origin_block_root() ||
            candidate.slot != binding.slot() ||
            signature.is_some() && candidate.signature != signature
        {
            return Err(RetrievalError::Association)
        }
        let mut staged = self.staging.lock().map_err(|_| RetrievalError::Closed)?;
        if staged.originals.is_empty() {
            return Ok(())
        }
        let Some((_, custody)) = archive.selection()? else {
            return Err(RetrievalError::Configuration);
        };
        let originals = staged
            .originals
            .values()
            .filter(|wire| wire.kind() != SidecarKind::Column || custody.contains(&wire.index()))
            .cloned()
            .collect::<Vec<_>>();
        let result = archive.retain_verified(&originals);
        if result.is_ok() {
            staged.clear()
        }
        self.check_health(&result);
        result
    }
    pub(super) fn retain_native_block(
        &self,
        admitted: &AdmittedState<'_>,
    ) -> Result<(), RetrievalError> {
        if self.archive.is_none() ||
            self.staging.lock().map_err(|_| RetrievalError::Closed)?.originals.is_empty()
        {
            return Ok(())
        }
        let signature = match admitted.signed_block() {
            SignedBlockData::Classic(signed) => match signed {
                SignedBeaconBlock::Phase0(block, _) => block.signature,
                SignedBeaconBlock::Altair(block, _) => block.signature,
                SignedBeaconBlock::Bellatrix(block, _) => block.signature,
                SignedBeaconBlock::Capella(block, _) => block.signature,
                SignedBeaconBlock::Deneb(block, _) => block.signature,
                SignedBeaconBlock::Electra(block, _) => block.signature,
                SignedBeaconBlock::Fulu(block, _) => block.signature,
                SignedBeaconBlock::Gloas(block, _) => block.signature,
            },
            SignedBlockData::Gloas(block, _) => block.signature,
        };
        self.commit_binding(admitted.binding(), Some(signature))
    }
    pub(super) fn retain_native_envelope(
        &self,
        verified: &VerifiedEnvelope<'_>,
    ) -> Result<(), RetrievalError> {
        // The envelope has no slot field. Its private native binding provides the exact
        // candidate slot and this block-root link; commit_binding checks the retained candidate.
        if verified.signed().message.beacon_block_root != verified.binding().origin_block_root() {
            return Err(RetrievalError::Association)
        }
        self.commit_binding(verified.binding(), None)
    }
    #[cfg(test)]
    pub(super) fn staged_bytes(&self) -> usize {
        self.staging.lock().unwrap().bytes
    }
}
