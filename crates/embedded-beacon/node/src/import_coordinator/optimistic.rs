//! Checkpoint optimism is a consensus observation, never a fabricated execution receipt.
//! One owner produces FCU targets and bounded retries; networking can follow checked CL heads.

use super::*;
use crate::runtime_engine::OptimisticForkchoice;
use reth_embedded_beacon_fork_choice::{store::optimistic::OptimisticError, ExecutionStatus};

impl Graph {
    pub(super) fn optimistic_execution_target(
        &self,
    ) -> Result<Option<(Fork, ForkchoiceState)>, ImportError> {
        let Self::Classic { store, .. } = self else {
            return Err(ImportError::UnsupportedTransition)
        };
        let snapshot = store.snapshot()?;
        let Some(head) = store.execution_target_hash(snapshot.head.root)? else { return Ok(None) };
        let fork = store.block(snapshot.head.root).ok_or(StoreError::UnknownRoot)?.schema_fork();
        // During optimism the CL checkpoints are consensus observations, not EL finality.
        // Zero safe/finalized requests let execution catch up without asserting their validity.
        let finalized = [0; 32];
        Ok(Some((
            fork,
            ForkchoiceState {
                head_block_hash: B256::from(head),
                safe_block_hash: B256::from(finalized),
                finalized_block_hash: B256::from(finalized),
            },
        )))
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
    pub(super) fn publish_optimistic(&mut self, graph: &mut Graph) -> Result<bool, ImportError> {
        if self.replaying {
            return Ok(false)
        }
        if self.engine.is_closed() {
            return Err(ImportError::EngineUncertain)
        }
        // A constructor's VALID label is not a real FCU acknowledgement. Nonzero safety
        // requires settled payloads and an original prior VALID from this same adapter.
        // Keep retrying a new SYNCING target without treating that earlier receipt as its VALID.
        let use_confirmation = self.limits.fast_confirmation &&
            matches!(&*graph, Graph::Classic { store, .. }
                if store.execution_optimism_settled()) &&
            self.last_verified_forkchoice
                .as_ref()
                .is_some_and(|receipt| receipt.is_bound_to(&self.engine));
        let execution = if use_confirmation {
            graph.execution_state_with_boundary(self.execution_boundary.as_ref())?
        } else {
            graph.optimistic_execution_target()?
        }
        .map(|(_, state)| {
            let fork = self
                .config
                .fork_at_slot(graph.current_slot())
                .map_err(|_| ImportError::Configuration)?
                .fork;
            Ok::<_, ImportError>((fork, state))
        })
        .transpose()?;
        self.source
            .prepare_snapshot(graph.snapshot()?)
            .map_err(|error| ImportError::Persistence(error.to_string()))?;
        let mut acknowledged = false;
        if let Some((fork, state)) = execution {
            let matching = self
                .last_verified_forkchoice
                .as_ref()
                .is_some_and(|receipt| receipt.matches(&self.engine, state));
            let retry = !matching && self.last_execution_retry_slot != Some(graph.current_slot());
            if self.last_execution_state != Some((fork, state)) || retry {
                let version = forkchoice_version(fork).ok_or(ImportError::Configuration)?;
                match self
                    .runtime
                    .block_on(self.engine.optimistic_forkchoice_updated(version, state, None))
                    .map_err(|error| ImportError::Forkchoice(error.to_string()))?
                {
                    OptimisticForkchoice::Syncing => {
                        tracing::debug!(target:"beacon::execution", head=?state.head_block_hash,
                            "[beacon] Consensus head retained while execution catches up");
                    }
                    OptimisticForkchoice::Valid(receipt) => {
                        let Graph::Classic { store, .. } = graph else {
                            return Err(ImportError::UnsupportedTransition)
                        };
                        let head = store.snapshot()?.head.root;
                        if ReadGossipView::execution_status(store, head) ==
                            ExecutionStatus::Optimistic
                        {
                            self.runtime
                                .block_on(
                                    store.refresh_execution_status(
                                        head,
                                        Arc::new(self.engine.clone()),
                                    ),
                                )
                                .map_err(optimistic_error)?;
                            if ReadGossipView::execution_status(store, head) !=
                                ExecutionStatus::Valid
                            {
                                return Err(ImportError::EngineUncertain)
                            }
                        }
                        self.last_verified_forkchoice = Some(receipt);
                        acknowledged = true;
                    }
                }
                self.last_execution_retry_slot = Some(graph.current_slot());
            }
        }
        // A pending finalized state must never replace the original trusted recovery boundary.
        // Once all retained payloads are VALID, the ordinary authenticated retention may resume.
        if let Graph::Classic { store, .. } = graph &&
            store.execution_optimism_settled() &&
            execution.as_ref().is_some_and(|(_, state)| {
                self.last_verified_forkchoice
                    .as_ref()
                    .is_some_and(|receipt| receipt.matches(&self.engine, *state))
            })
        {
            if store.prune_finalized_history(FinalizedRetentionPolicy::default())?.removed_blocks >
                0
            {
                graph.reset_confirmation()?;
            }
        }
        let snapshot = graph.snapshot()?;
        self.source
            .commit_snapshot(snapshot)
            .map_err(|error| ImportError::Persistence(error.to_string()))?;
        self.source
            .finish_retention(snapshot)
            .map_err(|error| ImportError::Persistence(error.to_string()))?;
        self.collect_after_publication(graph, execution.as_ref())?;
        self.last_execution_state = execution;
        Ok(acknowledged)
    }
}

pub(super) fn optimistic_error(error: OptimisticError) -> ImportError {
    match error {
        OptimisticError::Store(error) => ImportError::Consensus(error),
        OptimisticError::InvalidParent => ImportError::Consensus(StoreError::UnknownRoot),
        OptimisticError::ResourceLimit => ImportError::Bounds,
        OptimisticError::NotOptimisticCandidate => ImportError::Consensus(
            StoreError::ExecutionImport("not an optimistic import candidate".into()),
        ),
        other => ImportError::ExecutionBoundary(other.to_string()),
    }
}
