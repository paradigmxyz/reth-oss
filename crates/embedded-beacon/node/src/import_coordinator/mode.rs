//! Checked startup execution modes and ownership of one authenticated replay event.

use super::*;
use crate::compact_persistence::events::DurableEvent;

pub(super) enum StartupExecutionMode {
    Strict,
    Reconciled(Box<ReconciledCheckpoint>),
    Optimistic,
}

#[derive(Default)]
pub(super) struct ReplayContext {
    pub(super) started: bool,
    active: Option<ReplayEvent>,
}

impl ReplayContext {
    pub(super) fn event(&self) -> Option<&DurableEvent> {
        self.active.as_ref().map(|active| &active.event)
    }

    pub(super) fn begin(
        &mut self,
        event: DurableEvent,
        lease: Option<StateLease>,
    ) -> Result<(), ImportError> {
        if self.active.is_some() {
            return Err(ImportError::RecoveryRequired);
        }
        self.active = Some(ReplayEvent { event, _lease: lease });
        Ok(())
    }

    pub(super) fn finish(&mut self) {
        self.active = None;
    }
}

struct ReplayEvent {
    event: DurableEvent,
    // Retain the exact checked allocation for the entire serialized replay attempt.
    _lease: Option<StateLease>,
}
