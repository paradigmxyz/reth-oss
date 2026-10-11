//! Distinct dormant peer preparation from genuine genesis and sealed forward-import status.

use super::*;
use crate::{import_coordinator::AuthenticatedStatus, VerifiedGenesis};

impl CheckpointPeerRuntime {
    /// Prepare the actual persisted identity from this genesis owner's sealed graph status.
    /// Historical genesis remains an original trust input; no checkpoint proof is fabricated.
    #[allow(clippy::too_many_arguments)]
    pub fn open_genesis(
        repository_dir: impl AsRef<Path>,
        identity: StoreIdentity,
        config: ChainConfig,
        genesis: VerifiedGenesis,
        status: AuthenticatedStatus,
        now_unix_ms: u64,
        listeners: PeerListenConfig,
    ) -> Result<Self, PeerStartupError> {
        let digest = checked_config_digest(&config)?;
        let root = genesis.block_root();
        if genesis.config_digest() != digest ||
            status.config_digest() != digest ||
            status.genesis_origin() != Some(root)
        {
            return Err(PeerStartupError::UnverifiedGenesis)
        }
        let actual = status.snapshot();
        if (actual.finalized_checkpoint.epoch == 0 && actual.finalized_checkpoint.root != root) ||
            (actual.justified_checkpoint.epoch == 0 && actual.justified_checkpoint.root != root) ||
            (actual.head_slot == 0 && actual.head.root != root)
        {
            return Err(PeerStartupError::UnverifiedGenesis)
        }
        // Consumption independently rechecks this exact configuration and original immutable bytes.
        let _originals = genesis.into_import_inputs(&config)?;
        Self::open_status(
            repository_dir,
            identity,
            config,
            status.into_network(),
            now_unix_ms,
            listeners,
            Some(root),
        )
    }
}

#[cfg(test)]
mod tests;
