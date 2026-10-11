//! Optional optimistic import reuses the fully checked native follower and keeps views honest.

use super::*;
use crate::store::{
    optimistic::{
        self, CompactOptimisticTracking, ImportOutcome, OptimisticError, OptimisticParameters,
        OptimisticStore,
    },
    timing::elapsed_micros,
    BlockImportTimings,
};

impl CompactStore {
    /// Opt in at one authenticated checkpoint. The trusted CL anchor is assumed valid by
    /// checkpoint-sync rules; this method does not construct any Engine acknowledgement.
    pub fn enable_execution_optimism(
        &mut self,
        parameters: OptimisticParameters,
    ) -> Result<(), OptimisticError> {
        if self.optimistic.is_some() {
            return Err(OptimisticError::InvalidParameters)
        }
        self.optimistic = Some(CompactOptimisticTracking::trusted_anchor(&self.graph, parameters)?);
        Ok(())
    }

    /// Whether this graph uses explicit real Engine deferred-status tracking.
    pub const fn execution_optimism_enabled(&self) -> bool {
        self.optimistic.is_some()
    }

    /// Every retained execution branch has actual VALID (or trusted checkpoint identity).
    pub fn execution_optimism_settled(&self) -> bool {
        self.optimistic.as_ref().is_none_or(|tracking| tracking.all_valid())
    }

    /// Exact consensus-derived payload target for Engine catch-up, never an execution receipt.
    /// Invalid branches cannot provide a target; verified_execution_block_hash remains VALID-only.
    pub fn execution_target_hash(&self, root: Root) -> Result<Option<Root>, StoreError> {
        if self.graph.execution_status(root) == ExecutionStatus::Invalid {
            return Err(StoreError::ExecutionImport("execution target is invalid".into()))
        }
        let block = self.graph.block(root).ok_or(StoreError::UnknownRoot)?;
        let hash = match block {
            BeaconBlock::Phase0(..) | BeaconBlock::Altair(..) => return Ok(None),
            BeaconBlock::Bellatrix(block, _) => block.body.execution_payload.block_hash,
            BeaconBlock::Capella(block, _) => block.body.execution_payload.block_hash,
            BeaconBlock::Deneb(block, _) => block.body.execution_payload.block_hash,
            BeaconBlock::Electra(block, _) => block.body.execution_payload.block_hash,
            BeaconBlock::Fulu(block, _) => block.body.execution_payload.block_hash,
            BeaconBlock::Gloas(..) => return Err(StoreError::ValidationUnavailable(Fork::Gloas)),
        };
        Ok((hash != [0; 32]).then_some(hash))
    }

    /// Complete original consensus, BLS, transition, root and DA validation with typed optimism.
    /// Actual INVALID decisions fail closed before any compact publication or durable admission.
    pub async fn on_block_optimistic<
        E: optimistic::ExecutionEngine,
        D: crate::data_availability::DataAvailability,
    >(
        &mut self,
        signed: &SignedBeaconBlock,
        engine: Arc<E>,
        availability: &D,
    ) -> Result<ImportOutcome, OptimisticError> {
        self.on_block_optimistic_timed(signed, engine, availability)
            .await
            .map(|(outcome, _)| outcome)
    }

    /// Measure the same checked import, including deferred execution and durable retention.
    /// Timings observe work only; SYNCING and ACCEPTED remain optimistic and never imply VALID.
    pub async fn on_block_optimistic_timed<
        E: optimistic::ExecutionEngine,
        D: crate::data_availability::DataAvailability,
    >(
        &mut self,
        signed: &SignedBeaconBlock,
        engine: Arc<E>,
        availability: &D,
    ) -> Result<(ImportOutcome, BlockImportTimings), OptimisticError> {
        let started = Instant::now();
        let tracking = self.optimistic.clone().ok_or(OptimisticError::InvalidParameters)?;
        let mut follower = OptimisticStore::from_compact(self.graph.clone(), tracking, engine)?;
        let preparation_micros = elapsed_micros(started);
        let (outcome, mut timings) =
            follower.on_block_consensus_only_timed(signed, availability).await?;
        if outcome == ImportOutcome::ExecutionRejected {
            // An invalid candidate may invalidate earlier optimistic ancestors. The owner must
            // halt rather than retaining its previous graph as usable authority.
            return Ok((outcome, timings))
        }
        let started = Instant::now();
        let (graph, tracking) = follower.into_compact()?;
        let mut candidate = self.clone();
        candidate.graph = graph;
        candidate.optimistic = Some(tracking);
        timings.fork_choice_micros = timings
            .fork_choice_micros
            .saturating_add(preparation_micros)
            .saturating_add(elapsed_micros(started));
        let started = Instant::now();
        candidate.retain_block_and_availability(signed, availability)?;
        candidate.finish(self)?;
        timings.persistence_micros = elapsed_micros(started);
        Ok((outcome, timings))
    }

    /// Requery the exact original payload and apply the native VALID ancestry/INVALID rules.
    pub async fn refresh_execution_status<E: optimistic::ExecutionEngine>(
        &mut self,
        root: Root,
        engine: Arc<E>,
    ) -> Result<(), OptimisticError> {
        let tracking = self.optimistic.clone().ok_or(OptimisticError::InvalidParameters)?;
        let mut follower = OptimisticStore::from_compact(self.graph.clone(), tracking, engine)?;
        follower.refresh_execution_status(root).await?;
        let (graph, tracking) = follower.into_compact()?;
        let mut candidate = self.clone();
        candidate.graph = graph;
        candidate.optimistic = Some(tracking);
        candidate.finish(self)?;
        Ok(())
    }

    /// Migration may discard the overlay only after every retained real payload is VALID.
    pub fn finish_execution_optimism(&mut self) -> Result<(), OptimisticError> {
        if self.optimistic.as_ref().is_some_and(|tracking| !tracking.all_valid()) {
            return Err(OptimisticError::ExecutionDeferred)
        }
        self.optimistic = None;
        Ok(())
    }

    pub(super) fn admitted_execution_hash(&self, root: Root) -> Result<Option<Root>, StoreError> {
        if self.optimistic.is_some() &&
            self.graph.execution_status(root) == ExecutionStatus::Optimistic
        {
            Ok(None)
        } else {
            self.graph.verified_execution_block_hash(root)
        }
    }
}
