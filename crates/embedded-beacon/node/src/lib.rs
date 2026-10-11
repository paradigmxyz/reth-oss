//! Runtime readiness and trusted bootstrap checks for the embedded beacon.
//!
//! Full transition validation is a prerequisite for live sync. The prototype cannot override
//! this requirement using an operator flag or checksum-only archive validation.

#[cfg(feature = "gossip-validation")]
pub mod gloas_columns;

#[cfg(feature = "gossip-validation")]
pub mod gloas_bids;

#[cfg(feature = "gossip-validation")]
pub mod gloas_payload_attestations;

#[cfg(feature = "gossip-validation")]
pub mod fulu_columns;

#[cfg(feature = "gossip-validation")]
pub mod attestation_gossip;

#[cfg(feature = "gossip-validation")]
pub mod runtime_parameters;

#[cfg(feature = "gossip-validation")]
pub mod compact_persistence;

#[cfg(feature = "gossip-validation")]
pub mod blob_sidecars;

#[cfg(all(test, feature = "gossip-validation"))]
mod gossip_tests;

mod bootstrap;
mod checkpoint_store;
pub mod weak_subjectivity;

#[cfg(feature = "peer-runtime")]
pub mod peer_runtime;

#[cfg(feature = "execution-verification")]
pub mod engine_verification;

#[cfg(feature = "execution-verification")]
pub mod late_engine_verification;

#[cfg(feature = "execution-verification")]
pub mod runtime_engine;

#[cfg(feature = "execution-verification")]
pub mod terminal_verification;

#[cfg(all(
    feature = "peer-runtime",
    feature = "gossip-validation",
    feature = "execution-verification"
))]
mod execution_checkpoint_boundary;

#[cfg(all(
    feature = "peer-runtime",
    feature = "gossip-validation",
    feature = "execution-verification"
))]
pub mod import_coordinator;

#[cfg(all(
    feature = "peer-runtime",
    feature = "gossip-validation",
    feature = "execution-verification"
))]
pub mod execution_bootstrap;

pub use bootstrap::{BootstrapDecodeLimits, CheckpointBlockFormat, SszCheckpointVerifier};

#[cfg(feature = "gossip-validation")]
pub use bootstrap::{GenesisError, ReplayedCheckpoint, VerifiedGenesis};
pub use checkpoint_store::{CheckpointRepository, CheckpointStoreError, RecoveredCheckpoint};

use reth_embedded_beacon_primitives::{Checkpoint, Root, Slot};
use serde::Serialize;
use thiserror::Error;

/// Consensus components required before a live node may import anything.
#[derive(Debug, Clone, Serialize)]
pub struct Readiness {
    /// Intended current fork.
    pub target_fork: &'static str,
    /// Complete signed block, epoch and upgrade processing.
    pub full_state_transition: bool,
    /// Fork-specific store, event processing and head selection.
    pub full_fork_choice: bool,
    /// Gossip, discovery and request/response peer runtime.
    pub peer_runtime: bool,
    /// Trusted checkpoint state verification and initialization.
    pub checkpoint_bootstrap: bool,
    /// Verified state persistence and replay.
    pub verified_state_recovery: bool,
    /// Complete required conformance vectors.
    pub full_spec_conformance: bool,
}

impl Readiness {
    /// Describes current coverage, without inferring success from scaffolding.
    pub const fn current() -> Self {
        Self {
            target_fork: "gloas",
            full_state_transition: false,
            full_fork_choice: false,
            peer_runtime: false,
            checkpoint_bootstrap: false,
            verified_state_recovery: false,
            full_spec_conformance: false,
        }
    }

    /// Errors before starting a live node if any required validation path is missing.
    pub const fn require_live_sync(&self) -> Result<(), BootstrapError> {
        if !self.full_state_transition ||
            !self.full_fork_choice ||
            !self.peer_runtime ||
            !self.checkpoint_bootstrap ||
            !self.verified_state_recovery ||
            !self.full_spec_conformance
        {
            return Err(BootstrapError::IncompleteImplementation);
        }
        Ok(())
    }
}

/// A checkpoint validated by a complete state/block decoder.
#[derive(Debug, Clone)]
pub struct VerifiedCheckpoint {
    checkpoint: Checkpoint,
    state_root: Root,
    slot: Slot,
    // Set only by the concrete complete SSZ verifier, never by caller evidence.
    config_digest: Option<Root>,
}

impl VerifiedCheckpoint {
    /// Verifies checkpoint identity, block/state linkage and weak-subjectivity age.
    pub fn verify(
        verifier: &impl CheckpointVerifier,
        state_ssz: &[u8],
        block_ssz: &[u8],
        trusted: Checkpoint,
        settings: BootstrapSettings,
    ) -> Result<Self, BootstrapError> {
        let BootstrapSettings { network, slots_per_epoch, current_epoch, max_age_epochs } =
            settings;
        if slots_per_epoch == 0 || max_age_epochs == 0 {
            return Err(BootstrapError::InvalidConfiguration);
        }
        if trusted.epoch > current_epoch || current_epoch - trusted.epoch > max_age_epochs {
            return Err(BootstrapError::StaleCheckpoint);
        }
        let evidence = verifier.verify_state_and_block(state_ssz, block_ssz)?;
        let boundary = trusted
            .epoch
            .checked_mul(slots_per_epoch)
            .ok_or(BootstrapError::InvalidConfiguration)?;
        if evidence.genesis_validators_root != network {
            return Err(BootstrapError::WrongNetwork);
        }
        if evidence.block_root != trusted.root {
            return Err(BootstrapError::WrongTrustedRoot);
        }
        // A checkpoint state is at the epoch boundary; its latest block can precede an empty slot.
        if evidence.state_slot != boundary ||
            evidence.block_slot > evidence.state_slot ||
            evidence.latest_block_root != trusted.root
        {
            return Err(BootstrapError::StateBlockMismatch);
        }
        // A matching latest-block root does not authenticate an independently supplied
        // later state. Empty-slot advancement must be replayed from an authenticated
        // post-block state by the complete transition path before it can be accepted.
        if evidence.state_slot != evidence.block_slot ||
            evidence.state_root != evidence.block_state_root
        {
            return Err(BootstrapError::UnverifiedBoundaryState);
        }
        Ok(Self {
            checkpoint: trusted,
            state_root: evidence.state_root,
            slot: evidence.state_slot,
            config_digest: None,
        })
    }

    /// Complete SSZ verifier configuration identity, absent for generic caller evidence.
    #[cfg(any(feature = "gossip-validation", feature = "peer-runtime", test))]
    pub(crate) const fn config_digest(&self) -> Option<Root> {
        self.config_digest
    }

    /// Operator-supplied trusted checkpoint that was matched exactly.
    pub const fn checkpoint(&self) -> Checkpoint {
        self.checkpoint
    }
    /// Validated state Merkle root.
    pub const fn state_root(&self) -> Root {
        self.state_root
    }
    /// Checkpoint state slot.
    pub const fn slot(&self) -> Slot {
        self.slot
    }
}

/// Explicit network and trust-window configuration for checkpoint verification.
#[derive(Debug, Clone, Copy)]
pub struct BootstrapSettings {
    /// Expected consensus network identity.
    pub network: Root,
    /// Epoch length from the validated network preset.
    pub slots_per_epoch: u64,
    /// Current epoch from the validated consensus clock.
    pub current_epoch: u64,
    /// Operator trust-age cap; the complete implementation must also enforce the
    /// state-dependent weak-subjectivity period.
    pub max_age_epochs: u64,
}

/// Complete schema decoding, state hashing and block/state linkage verification.
pub trait CheckpointVerifier {
    /// Must decode and hash the correct fork schemas, including the state root
    /// committed by the trusted block. Checksums and latest-header identity alone
    /// cannot authenticate an independently supplied state.
    fn verify_state_and_block(
        &self,
        state_ssz: &[u8],
        block_ssz: &[u8],
    ) -> Result<CheckpointEvidence, BootstrapError>;
}

/// Evidence returned only after fork-correct SSZ decoding and root computation.
#[derive(Debug, Clone)]
pub struct CheckpointEvidence {
    /// Genesis validators root decoded from the state.
    pub genesis_validators_root: Root,
    /// Hash-tree-root of the state.
    pub state_root: Root,
    /// State slot.
    pub state_slot: Slot,
    /// Root of the latest block represented by the state.
    pub latest_block_root: Root,
    /// Block hash-tree-root.
    pub block_root: Root,
    /// State root committed by the decoded block.
    pub block_state_root: Root,
    /// Block slot.
    pub block_slot: Slot,
}

/// Bootstrap validation failures.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum BootstrapError {
    /// Live consensus validation is not fully implemented.
    #[error("live beacon sync unavailable: full Gloas transitions, fork choice, peer runtime, verified bootstrap/recovery and conformance remain required")]
    IncompleteImplementation,
    /// Invalid operator limits.
    #[error("invalid bootstrap configuration")]
    InvalidConfiguration,
    /// Checkpoint is future or older than the configured trust window.
    #[error("checkpoint is outside the trusted age window")]
    StaleCheckpoint,
    /// Cross-network bootstrap.
    #[error("checkpoint belongs to another consensus network")]
    WrongNetwork,
    /// Trusted block root mismatch.
    #[error("checkpoint block does not match trusted root")]
    WrongTrustedRoot,
    /// State/header linkage or boundary mismatch.
    #[error("checkpoint state and block do not match")]
    StateBlockMismatch,
    /// Boundary state is not directly committed by the trusted block; authenticated
    /// empty-slot replay is not available yet.
    #[error("checkpoint boundary state is not authenticated by the trusted block")]
    UnverifiedBoundaryState,
    /// Authenticated empty-slot replay exceeds the explicit bounded work policy.
    #[error("checkpoint empty-slot replay exceeds its configured slot limit")]
    CheckpointReplayLimit,
    /// The exact scheduled state transition rejected authenticated checkpoint replay.
    #[error("checkpoint empty-slot replay failed: {0}")]
    CheckpointReplay(String),
    /// Checkpoint input exceeds the explicit decoder policy.
    #[error("checkpoint SSZ exceeds the configured input limit")]
    CheckpointTooLarge,
    /// The selected complete schema rejected the supplied bytes.
    #[error("invalid checkpoint SSZ: {0}")]
    InvalidCheckpointSsz(String),
    /// State fork metadata disagrees with the configured activation.
    #[error("checkpoint state fork metadata disagrees with the network schedule")]
    StateForkMismatch,
    /// State-dependent checkpoint trust period or active registry authentication failed.
    #[error(transparent)]
    WeakSubjectivity(#[from] weak_subjectivity::WeakSubjectivityError),
    /// Unsupported full SSZ schema.
    #[error("complete state/block decoding for this fork is unavailable")]
    UnsupportedSchema,
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FakeVerifier(CheckpointEvidence);
    impl CheckpointVerifier for FakeVerifier {
        fn verify_state_and_block(
            &self,
            _: &[u8],
            _: &[u8],
        ) -> Result<CheckpointEvidence, BootstrapError> {
            Ok(self.0.clone())
        }
    }

    #[test]
    fn checkpoint_verification_requires_a_directly_committed_boundary_state() {
        let evidence = CheckpointEvidence {
            genesis_validators_root: [1; 32],
            state_root: [2; 32],
            state_slot: 64,
            latest_block_root: [3; 32],
            block_root: [3; 32],
            block_state_root: [2; 32],
            block_slot: 64,
        };
        let trusted = Checkpoint { epoch: 2, root: [3; 32] };
        let good = VerifiedCheckpoint::verify(
            &FakeVerifier(evidence.clone()),
            &[],
            &[],
            trusted,
            BootstrapSettings {
                network: [1; 32],
                slots_per_epoch: 32,
                current_epoch: 3,
                max_age_epochs: 10,
            },
        )
        .unwrap();
        assert_eq!(good.slot(), 64);
        assert_eq!(good.state_root(), [2; 32]);
        assert_eq!(good.config_digest(), None);
        assert_eq!(
            VerifiedCheckpoint::verify(
                &FakeVerifier(evidence.clone()),
                &[],
                &[],
                trusted,
                BootstrapSettings {
                    network: [9; 32],
                    slots_per_epoch: 32,
                    current_epoch: 3,
                    max_age_epochs: 10
                }
            )
            .unwrap_err(),
            BootstrapError::WrongNetwork
        );
        assert_eq!(
            VerifiedCheckpoint::verify(
                &FakeVerifier(evidence),
                &[],
                &[],
                trusted,
                BootstrapSettings {
                    network: [1; 32],
                    slots_per_epoch: 32,
                    current_epoch: 20,
                    max_age_epochs: 10
                }
            )
            .unwrap_err(),
            BootstrapError::StaleCheckpoint
        );
    }

    #[test]
    fn checkpoint_requires_exact_trusted_root_and_epoch_boundary() {
        let mut evidence = CheckpointEvidence {
            genesis_validators_root: [1; 32],
            state_root: [2; 32],
            state_slot: 64,
            latest_block_root: [3; 32],
            block_root: [3; 32],
            block_state_root: [2; 32],
            block_slot: 64,
        };
        let trusted = Checkpoint { epoch: 2, root: [3; 32] };
        let settings = BootstrapSettings {
            network: [1; 32],
            slots_per_epoch: 32,
            current_epoch: 3,
            max_age_epochs: 10,
        };
        evidence.block_root = [4; 32];
        assert_eq!(
            VerifiedCheckpoint::verify(
                &FakeVerifier(evidence.clone()),
                &[],
                &[],
                trusted,
                settings
            )
            .unwrap_err(),
            BootstrapError::WrongTrustedRoot
        );
        evidence.block_root = [3; 32];
        evidence.state_slot = 63;
        assert_eq!(
            VerifiedCheckpoint::verify(
                &FakeVerifier(evidence.clone()),
                &[],
                &[],
                trusted,
                settings
            )
            .unwrap_err(),
            BootstrapError::StateBlockMismatch
        );
        evidence.state_slot = 64;
        evidence.latest_block_root = [4; 32];
        assert_eq!(
            VerifiedCheckpoint::verify(&FakeVerifier(evidence), &[], &[], trusted, settings)
                .unwrap_err(),
            BootstrapError::StateBlockMismatch
        );
    }

    #[test]
    fn copied_trusted_header_cannot_authenticate_tampered_state() {
        let evidence = CheckpointEvidence {
            genesis_validators_root: [1; 32],
            state_root: [9; 32],
            state_slot: 64,
            latest_block_root: [3; 32],
            block_root: [3; 32],
            block_state_root: [2; 32],
            block_slot: 64,
        };
        assert_eq!(
            VerifiedCheckpoint::verify(
                &FakeVerifier(evidence),
                &[],
                &[],
                Checkpoint { epoch: 2, root: [3; 32] },
                BootstrapSettings {
                    network: [1; 32],
                    slots_per_epoch: 32,
                    current_epoch: 3,
                    max_age_epochs: 10,
                },
            )
            .unwrap_err(),
            BootstrapError::UnverifiedBoundaryState,
        );
    }

    #[test]
    fn skipped_boundary_requires_authenticated_transition_replay() {
        let evidence = CheckpointEvidence {
            genesis_validators_root: [1; 32],
            state_root: [2; 32],
            state_slot: 64,
            latest_block_root: [3; 32],
            block_root: [3; 32],
            block_state_root: [2; 32],
            block_slot: 63,
        };
        assert_eq!(
            VerifiedCheckpoint::verify(
                &FakeVerifier(evidence),
                &[],
                &[],
                Checkpoint { epoch: 2, root: [3; 32] },
                BootstrapSettings {
                    network: [1; 32],
                    slots_per_epoch: 32,
                    current_epoch: 3,
                    max_age_epochs: 10,
                },
            )
            .unwrap_err(),
            BootstrapError::UnverifiedBoundaryState,
        );
    }
    #[test]
    fn incomplete_consensus_cannot_launch() {
        assert_eq!(
            Readiness::current().require_live_sync().unwrap_err(),
            BootstrapError::IncompleteImplementation
        );
        assert_eq!(Readiness::current().target_fork, "gloas");
    }
}

#[cfg(feature = "checkpoint-http")]
pub mod checkpoint_download;

#[cfg(all(
    feature = "peer-runtime",
    feature = "gossip-validation",
    feature = "execution-verification"
))]
pub mod live_sync;

#[cfg(feature = "beacon-api")]
pub mod beacon_api;
