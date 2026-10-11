//! Wall-clock observations of checked classic import phases; timings confer no validation
//! authority.

use std::time::Instant;

/// Independent phase durations of a completed, consensus-verified block import.
///
/// Transition time includes the mandatory execution callback; genuinely deferred responses remain
/// unvalidated. Graph/status time may include terminal ancestry feedback and is not pure CPU time.
/// Persistence includes retention,
/// durable state writes, cache admission, cooling and compact snapshot publication.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BlockImportTimings {
    /// Required data retrieval and availability verification, in microseconds.
    pub availability_micros: u64,
    /// State transition, signatures and the mandatory execution callback, in microseconds.
    pub state_transition_micros: u64,
    /// Native graph, checkpoints and execution-status feedback wall-clock time, in microseconds.
    pub fork_choice_micros: u64,
    /// Compact retention and publication, in microseconds.
    pub persistence_micros: u64,
    /// Exact optimistic transition payload callback wait, included in transition time.
    /// This records a real callback completion, including deferred Engine responses; it grants
    /// no execution authority. Strict imports measure their captured proxy separately.
    pub execution_wait_micros: Option<u64>,
}

pub(super) fn elapsed_micros(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX)
}
