//! Optional beacon byte maintenance is gated by this owner's actual successful publication.
//!
//! Replaying, pending inputs and any failed or uncertain Engine/local publication never reach
//! this hook. A prepared pointer or a public VALID flag is insufficient deletion authority.

use super::*;

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
    /// Called only at the end of publish, after every canonical and cleanup operation succeeds.
    pub(super) fn collect_after_publication(
        &self,
        graph: &Graph,
        execution: Option<&(Fork, ForkchoiceState)>,
    ) -> Result<(), ImportError> {
        if !self.publication_collection_allowed(execution) {
            return Ok(())
        }
        self.collect_after_publication_with_snapshot(graph, execution, graph.snapshot()?)
    }

    /// The coordinator supplies metadata from the same graph after its final mutation.
    pub(super) fn collect_after_publication_with_snapshot(
        &self,
        graph: &Graph,
        execution: Option<&(Fork, ForkchoiceState)>,
        snapshot: StoreSnapshot,
    ) -> Result<(), ImportError> {
        if !self.publication_collection_allowed(execution) {
            return Ok(())
        }
        if let Some(report) = self
            .repository
            .collect_published_beacon_objects(snapshot, graph.current_slot())
            .map_err(|error| ImportError::Persistence(error.to_string()))? &&
            report.removed > 0
        {
            tracing::debug!(target: "beacon::storage",
                slot = graph.current_slot(), examined = report.examined, removed = report.removed,
                removed_bytes = report.removed_bytes, journal_objects = report.journal_objects,
                journal_commit_micros = report.journal_commit_micros,
                "[beacon] Collected unowned beacon bytes after canonical publication");
        }
        Ok(())
    }

    fn publication_collection_allowed(&self, execution: Option<&(Fork, ForkchoiceState)>) -> bool {
        !self.replaying &&
            self.recovery_execution_boundary.is_none() &&
            !self.engine.is_closed() &&
            execution.is_none_or(|(_, state)| {
                self.last_verified_forkchoice
                    .as_ref()
                    .is_some_and(|receipt| receipt.matches(&self.engine, *state))
            }) &&
            self.column_retention
                .as_ref()
                .is_none_or(|retention| retention.ensure_healthy().is_ok())
    }
}
