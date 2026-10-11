//! Optional fast confirmation observes real timely slots and never invents catch-up history.

use super::*;
use reth_embedded_beacon_fork_choice::fast_confirmation::Parameters;
use reth_embedded_beacon_primitives::Checkpoint;

/// Disabled and suspended confirmation both fall back to the graph's actual finalized root.
#[derive(Clone, Debug)]
pub(super) struct Confirmation<V: FastConfirmationView> {
    enabled: bool,
    pub(super) state: Option<FastConfirmation<V>>,
}

impl<V: FastConfirmationView> Confirmation<V> {
    pub(super) const fn new(enabled: bool) -> Self {
        Self { enabled, state: None }
    }

    fn checkpoints(&self, graph: &V) -> Result<Vec<Checkpoint>, StoreError> {
        if let Some(state) = &self.state {
            state.checkpoint_states_for_update(graph)
        } else {
            let (justified, finalized) = graph.checkpoints();
            Ok(vec![justified, finalized])
        }
    }

    fn observe(&mut self, graph: &V) -> Result<(), StoreError> {
        let mut next = if let Some(state) = &self.state {
            state.clone()
        } else {
            FastConfirmation::new(graph, Parameters::default())?
        };
        next.on_fast_confirmation(graph)?;
        self.state = Some(next);
        Ok(())
    }
}

impl Graph {
    pub(super) fn confirmation_enabled(&self) -> bool {
        match self {
            Self::Classic { confirmation, .. } => confirmation.enabled,
            Self::Gloas { confirmation, .. } => confirmation.enabled,
        }
    }

    fn confirmation_at_slot_start(&mut self) -> Result<(), ImportError> {
        match self {
            Self::Classic { store, confirmation } => {
                // Preview epoch rotation before acquiring leases for its checkpoint sources.
                let metadata = store.prepare_fast_confirmation(&[])?;
                let checkpoints = confirmation.checkpoints(&metadata)?;
                let prepared = store.prepare_fast_confirmation(&checkpoints)?;
                confirmation.observe(&prepared)?;
            }
            Self::Gloas { store, confirmation } => {
                let metadata = store.prepare_fast_confirmation(&[])?;
                let checkpoints = confirmation.checkpoints(&metadata)?;
                let prepared = store.prepare_fast_confirmation(&checkpoints)?;
                confirmation.observe(&prepared)?;
            }
        }
        Ok(())
    }

    pub(super) fn reset_confirmation(&mut self) -> Result<(), ImportError> {
        match self {
            Self::Classic { confirmation, .. } => confirmation.state = None,
            Self::Gloas { confirmation, .. } => confirmation.state = None,
        }
        Ok(())
    }

    pub(super) fn observe_confirmation(
        &mut self,
        config: &ChainConfig,
        prior: u64,
        target: u64,
        now: u64,
    ) -> Result<(), ImportError> {
        if !self.confirmation_enabled() {
            return Ok(())
        }
        let snapshot = self.snapshot()?;
        let execution_pending = matches!(self, Self::Classic { store, .. }
            if store.execution_optimism_enabled() && !store.execution_optimism_settled());
        // Historical imports cannot establish the timely delivery assumed by FCR.
        if execution_pending || snapshot.head_slot.saturating_add(1) < target {
            self.reset_confirmation()?;
            return Ok(())
        }
        let clock = config.clock().map_err(|_| ImportError::Configuration)?;
        let start = clock.start_of_slot(target).map_err(|_| ImportError::Clock)?;
        let (parameters, due_bps) = match self {
            Self::Classic { store, .. } => {
                let parameters = ReadGossipView::store_parameters(store);
                (parameters, parameters.attestation_due_bps)
            }
            Self::Gloas { store, .. } => {
                let parameters = ReadGloasGossipView::store_parameters(store);
                (parameters, parameters.gloas_attestation_due_bps)
            }
        };
        let due =
            parameters.slot_duration_ms.checked_mul(due_bps).ok_or(ImportError::Clock)? / 10_000;
        if target - prior == 1 && now.checked_sub(start).ok_or(ImportError::Clock)? < due {
            self.confirmation_at_slot_start()?;
            tracing::debug!(target: "beacon::fcr", slot = target,
                "[beacon] Observed timely fast-confirmation slot");
        } else {
            // Keep ordinary fork choice and finality intact after a real missed deadline.
            self.reset_confirmation()?;
            tracing::debug!(target: "beacon::fcr", prior_slot = prior, current_slot = target,
                "[beacon] Fast confirmation suspended after missed observation; using finalized safety");
        }
        Ok(())
    }
}
