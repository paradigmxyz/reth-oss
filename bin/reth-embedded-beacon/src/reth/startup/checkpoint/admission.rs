//! Select checkpoint execution admission without creating an execution-validity receipt.

use reth_embedded_beacon_primitives::Fork;
use std::future::Future;

/// Native optimistic import is supported for payload-bearing classic checkpoint anchors.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum CheckpointAdmission {
    /// A checked sealed execution receipt is required before the strict owner starts.
    Strict,
    /// The checked importer owns all initial and advancing execution reconciliation.
    Optimistic,
}

impl CheckpointAdmission {
    /// Earlier anchors and Gloas retain their existing strict bootstrap requirements.
    pub(super) fn for_fork(fork: Fork) -> Self {
        if (Fork::Bellatrix..=Fork::Fulu).contains(&fork) {
            Self::Optimistic
        } else {
            Self::Strict
        }
    }

    /// Poll only the strict route's reconciliation future. Returning no receipt grants no
    /// execution validity; the caller must use the dedicated optimistic importer entry point.
    pub(super) async fn receipt<R>(
        self,
        reconciliation: impl Future<Output = eyre::Result<R>>,
    ) -> eyre::Result<Option<R>> {
        match self {
            Self::Strict => reconciliation.await.map(Some),
            Self::Optimistic => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests;
