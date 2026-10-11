//! Transient queue pressure grants no graph authority and does not stop peer IO.
//! Original mutable inputs remain under their existing bounded live-owner reservation.

use super::*;

pub(super) fn observation<T>(result: Result<T, ImportError>) -> Result<Option<T>, LiveSyncError> {
    match result {
        Ok(value) => Ok(Some(value)),
        Err(ImportError::Backpressure) => {
            tracing::debug!(target: "beacon::sync",
                "[beacon] Deferring graph observation while import admission is busy");
            Ok(None)
        }
        Err(error) => Err(error.into()),
    }
}

pub(super) async fn contains_block(
    importer: &ImportCoordinator,
    root: Root,
) -> Result<Option<bool>, LiveSyncError> {
    // Queue pressure is an unavailable observation, never a claim that a root is missing.
    observation(importer.contains_block(root).await)
}

// Background custody can acquire the single candidate while block IO or owner reads await.
// Contention grants no availability and never replaces the existing candidate reservation.
pub(super) fn candidate<T>(result: Result<T, RetrievalError>) -> Result<Option<T>, LiveSyncError> {
    match result {
        Ok(candidate) => Ok(Some(candidate)),
        Err(RetrievalError::Capacity) => {
            tracing::debug!(target: "beacon::sync",
                "[beacon] Deferring original input while availability candidate admission is busy");
            Ok(None)
        }
        Err(error) => Err(error.into()),
    }
}
