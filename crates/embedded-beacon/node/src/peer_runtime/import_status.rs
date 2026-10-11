//! Sealed graph-derived peer advertisements; no public snapshot can manufacture authority.

use super::*;
use crate::import_coordinator::AuthenticatedStatus;

impl CheckpointPeerRuntime {
    /// Open with the actual graph's separately authenticated head, checkpoints and clock.
    /// This preserves an earlier genuine head slot when the trusted boundary skipped slots.
    #[allow(clippy::too_many_arguments)]
    pub fn open_authenticated(
        repository_dir: impl AsRef<Path>,
        identity: StoreIdentity,
        config: ChainConfig,
        checkpoint: VerifiedCheckpoint,
        status: AuthenticatedStatus,
        now_unix_ms: u64,
        listeners: PeerListenConfig,
    ) -> Result<Self, PeerStartupError> {
        if status.config_digest() != checked_config_digest(&config)? {
            return Err(PeerStartupError::UnverifiedCheckpoint)
        }
        let clock = config.clock()?;
        let current_slot = clock.slot_at(now_unix_ms).ok_or(PeerStartupError::Configuration)?;
        if status.current_slot() > current_slot {
            return Err(PeerStartupError::ClockRegression)
        }
        // The sealed graph clock may lag while validation or blocking startup completes.
        // Keep its head and finality unchanged; live startup refreshes status before peer IO.
        let status_time = clock
            .start_of_slot(status.current_slot())
            .map_err(|_| PeerStartupError::Configuration)?;
        Self::open_inner(
            repository_dir,
            identity,
            config,
            checkpoint,
            status_time,
            listeners,
            Some(status.into_network()),
        )
    }

    /// Publish only the token minted by the real owner after strict graph admission.
    /// This method persists synchronously and must be called off async IO workers.
    pub fn apply_authenticated_status(
        &mut self,
        status: AuthenticatedStatus,
    ) -> Result<(), PeerStartupError> {
        self.identity.ensure_healthy()?;
        if status.config_digest() != self.identity.digest ||
            status.genesis_origin() != self.genesis_origin
        {
            return Err(PeerStartupError::UnverifiedCheckpoint)
        }
        let trusted = status.into_network();
        let slot = trusted.current_slot;
        if slot < self.current_slot ||
            trusted.finalized.epoch < self.trusted_status.finalized.epoch ||
            (trusted.finalized.epoch == self.trusted_status.finalized.epoch &&
                trusted.finalized.root != self.trusted_status.finalized.root)
        {
            return Err(PeerStartupError::ClockRegression)
        }
        let published = trusted.clone();
        let mut persistence_error = None;
        let store = &mut self.identity;
        let policy = self.listeners.policy;
        let result = self.peer.update_snapshot(trusted, |record| {
            store.persist(record, slot, policy).map_err(|error| {
                persistence_error = Some(error);
                DiscoveryError::Persistence
            })
        });
        if let Some(error) = persistence_error {
            self.shutdown();
            return Err(error)
        }
        result?;
        if let Err(error) = self.identity.persist(&self.peer.local_enr(), slot, policy) {
            self.shutdown();
            return Err(error)
        }
        self.current_slot = slot;
        self.trusted_status = published;
        Ok(())
    }

    /// Admit only the first canonical Hoodi checkpoint experiment using exact sealed status.
    /// This repeats ordinary checkpoint binding and changes no readiness or import authority.
    #[allow(clippy::too_many_arguments)]
    pub fn open_experimental_hoodi_checkpoint(
        repository_dir: impl AsRef<Path>,
        identity: StoreIdentity,
        config: ChainConfig,
        checkpoint: VerifiedCheckpoint,
        status: AuthenticatedStatus,
        now_unix_ms: u64,
        listeners: PeerListenConfig,
    ) -> Result<Self, PeerStartupError> {
        validate_experimental_admission(&config, listeners, status.genesis_origin())?;
        let mut peer = Self::open_authenticated(
            repository_dir,
            identity,
            config,
            checkpoint,
            status,
            now_unix_ms,
            listeners,
        )?;
        peer.experimental_sync = true;
        Ok(peer)
    }
}
