//! Prepared, acknowledged and committed canonical publication on the single graph owner.

use super::*;
use crate::execution_checkpoint_boundary::roll_forward::PreparedExecutionBoundary;

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
    pub(super) fn publish(&mut self, graph: &mut Graph) -> Result<bool, ImportError> {
        self.publish_with_snapshot(graph).map(|(dispatched, _)| dispatched)
    }

    pub(super) fn publish_with_snapshot(
        &mut self,
        graph: &mut Graph,
    ) -> Result<(bool, StoreSnapshot), ImportError> {
        if self.optimistic && matches!(graph, Graph::Classic { .. }) {
            let dispatched = self.publish_optimistic(graph)?;
            return Ok((dispatched, graph.snapshot()?))
        }
        if self.replaying {
            return Ok((false, graph.snapshot()?))
        }
        let prepared = self.prepare_publication(graph)?;
        let acknowledged = self.acknowledge_publication(prepared)?;
        self.commit_publication(graph, acknowledged)
    }

    fn prepare_publication(&self, graph: &mut Graph) -> Result<PreparedPublication, ImportError> {
        if let Some(boundary) = &self.execution_boundary {
            boundary.check_adapter(&self.config, &self.engine).map_err(execution_boundary_error)?;
        }
        if self.engine.is_closed() {
            return Err(ImportError::EngineUncertain)
        }
        let policy = FinalizedRetentionPolicy::default();
        let mut prepared = match (&self.execution_boundary, &*graph) {
            (Some(boundary), Graph::Gloas { store, .. }) => {
                // Gloas roll-forward binds the graph before finalized pruning. Classic
                // publication does not consume this value and need not select another head.
                let expected_execution = graph
                    .execution_state_with_boundary(Some(boundary))
                    .map_err(|error| ImportError::PublicationInfrastructure(error.to_string()))?
                    .map(|(_, state)| state);
                boundary
                    .prepare_finalized_roll_forward(
                        &self.config,
                        store,
                        policy,
                        expected_execution,
                        &self.engine,
                    )
                    .map_err(execution_boundary_error)?
            }
            _ => None,
        };
        // A restart candidate cannot prune its prepared seed before the exact later-head
        // acknowledgement activates it. Normal finalized retirement resumes on the next step.
        let mut confirmation_reset = false;
        if self.recovery_execution_boundary.is_none() {
            let removed_blocks = match graph {
                Graph::Classic { store, .. } => {
                    store.prune_finalized_history(policy)?.removed_blocks
                }
                Graph::Gloas { store, .. } => {
                    if prepared.is_some() {
                        store.prune_finalized_consensus_history(policy)?.removed_blocks
                    } else {
                        store.prune_finalized_history(policy)?.removed_blocks
                    }
                }
            };
            if removed_blocks > 0 {
                graph.reset_confirmation()?;
                confirmation_reset = true;
            }
        }
        let candidate_boundary = prepared
            .as_ref()
            .map(|prepared| prepared.candidate())
            .or(self.execution_boundary.as_ref())
            .or_else(|| {
                self.recovery_execution_boundary.as_ref().map(|boundary| boundary.candidate())
            });
        // All graph mutations for this publication are complete. Engine acknowledgement,
        // journal commits and byte collection cannot change the native selected graph head.
        // Reuse its exact metadata instead of repeatedly scoring the same vote map.
        let snapshot = graph.snapshot()?;
        let execution = graph
            .execution_state_with_snapshot(candidate_boundary, snapshot)
            .map_err(|error| ImportError::PublicationInfrastructure(error.to_string()))?
            .map(|(_, state)| {
                let fork = self
                    .config
                    .fork_at_slot(graph.current_slot())
                    .map_err(|_| ImportError::Configuration)?
                    .fork;
                Ok::<_, ImportError>((fork, state))
            })
            .transpose()?;
        if confirmation_reset &&
            let (Some(prepared), Graph::Gloas { store, .. }) = (&mut prepared, &*graph)
        {
            prepared
                .rebind_finalized_confirmation(
                    &self.config,
                    store,
                    &self.engine,
                    execution.as_ref().map(|(_, state)| *state),
                )
                .map_err(execution_boundary_error)?;
        }
        self.source
            .prepare_snapshot(snapshot)
            .map_err(|error| ImportError::Persistence(error.to_string()))?;
        Ok(PreparedPublication { snapshot, execution, boundary: prepared })
    }

    fn acknowledge_publication(
        &mut self,
        prepared: PreparedPublication,
    ) -> Result<AcknowledgedPublication, ImportError> {
        let execution = prepared.execution;
        let dispatched = if let Some((fork, state)) = execution &&
            self.last_execution_state.as_ref() != Some(&(fork, state))
        {
            let version = forkchoice_version(fork).ok_or(ImportError::Configuration)?;
            // No attributes or invented custody declaration. An attempted failure may already
            // have changed the remote canonical EL chain; halt instead of retrying or rolling back.
            let receipt = self
                .runtime
                .block_on(self.engine.forkchoice_updated_receipt(version, state, None))
                .map_err(|error| ImportError::Forkchoice(error.to_string()))?;
            self.last_verified_forkchoice = Some(receipt);
            true
        } else {
            false
        };
        Ok(AcknowledgedPublication { prepared, dispatched })
    }

    fn commit_publication(
        &mut self,
        graph: &Graph,
        acknowledged: AcknowledgedPublication,
    ) -> Result<(bool, StoreSnapshot), ImportError> {
        let AcknowledgedPublication { prepared, dispatched } = acknowledged;
        let PreparedPublication { snapshot, execution, boundary: prepared } = prepared;
        self.source
            .commit_snapshot(snapshot)
            .map_err(|error| ImportError::Persistence(error.to_string()))?;
        if let Some(prepared) = prepared {
            self.execution_boundary = Some(
                prepared
                    .commit(
                        &self.config,
                        &self.engine,
                        execution.as_ref().and(self.last_verified_forkchoice.as_ref()),
                        &self.repository,
                    )
                    .map_err(|error| ImportError::PublicationInfrastructure(error.to_string()))?,
            );
        }
        if let Some(prepared) = self.recovery_execution_boundary.take() {
            let Graph::Gloas { store, .. } = graph else {
                return Err(ImportError::RecoveryRequired)
            };
            self.execution_boundary = Some(
                prepared
                    .commit(
                        &self.config,
                        store,
                        snapshot,
                        execution.as_ref().map(|(_, state)| *state),
                        execution.as_ref().and(self.last_verified_forkchoice.as_ref()),
                        &self.repository,
                        &self.engine,
                    )
                    .map_err(execution_boundary_error)?,
            );
        }
        self.source
            .finish_retention(snapshot)
            .map_err(|error| ImportError::Persistence(error.to_string()))?;
        self.collect_after_publication_with_snapshot(graph, execution.as_ref(), snapshot)?;
        self.last_execution_state = execution;
        Ok((dispatched, snapshot))
    }
}

struct PreparedPublication {
    snapshot: StoreSnapshot,
    execution: Option<(Fork, ForkchoiceState)>,
    boundary: Option<PreparedExecutionBoundary>,
}

// Engine effects cannot be rolled back by dropping this value. Failures still halt the owner.
struct AcknowledgedPublication {
    prepared: PreparedPublication,
    dispatched: bool,
}
