//! Status admission checks against a trusted chain configuration and finalized history.
//! A compatible wire status is not proof of a peer's head or execution validity.

use super::StatusV2;
use reth_embedded_beacon_config::ChainConfig;
use reth_embedded_beacon_primitives::{Checkpoint, Epoch, Root, Slot};

/// Concrete status admission failures.
#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum HandshakeError {
    /// The local trusted configuration or finalized-history inputs are inconsistent.
    #[error("invalid local handshake configuration")]
    Configuration,
    /// The remote network digest differs at the current wall-clock epoch.
    #[error("peer belongs to a different network or current fork")]
    ForkDigest,
    /// A peer's known finalized checkpoint contradicts canonical finalized history.
    #[error("peer finalized root conflicts with canonical history")]
    FinalizedRoot,
    /// The negotiated status version does not match the current fork.
    #[error("peer uses an inactive status version")]
    StatusVersion,
    /// A finalized checkpoint is future, or a carried checkpoint conflicts with the older head.
    #[error("inconsistent peer finalized epoch")]
    FinalizedEpoch,
}

/// What has actually been verified about the advertised finalized checkpoint.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StatusCompatibility {
    /// The advertised epoch/root agrees with supplied canonical finalized history.
    KnownFinality,
    /// The checkpoint is not in trusted history and must be resolved before trusting it.
    NeedsFinalizedRoot {
        /// Advertised finalized epoch.
        epoch: Epoch,
        /// Advertised consensus root, still unverified.
        root: Root,
    },
}

/// Validate status using the current wall-clock slot, rather than the peer's head slot.
/// `canonical_roots` must come from already validated finalized local history.
/// A returned unknown checkpoint cannot authorize imports, fork choice or readiness.
pub fn validate_status(
    config: &ChainConfig,
    current_slot: Slot,
    local_finalized: Checkpoint,
    canonical_roots: &[(Epoch, Root)],
    peer: &StatusV2,
) -> Result<StatusCompatibility, HandshakeError> {
    config.validate().map_err(|_| HandshakeError::Configuration)?;
    let current_epoch = current_slot / config.slots_per_epoch;
    if local_finalized.epoch > current_epoch ||
        canonical_roots.iter().any(|(epoch, root)| {
            *epoch > local_finalized.epoch ||
                (*epoch == local_finalized.epoch && *root != local_finalized.root)
        }) ||
        canonical_roots.iter().enumerate().any(|(index, (epoch, root))| {
            canonical_roots[..index]
                .iter()
                .any(|(other_epoch, other_root)| epoch == other_epoch && root != other_root)
        })
    {
        return Err(HandshakeError::Configuration);
    }
    let digest = config.fork_digest(current_epoch).map_err(|_| HandshakeError::Configuration)?;
    if peer.status.fork_digest != digest {
        return Err(HandshakeError::ForkDigest);
    }
    let finalized_epoch = peer.status.finalized_epoch;
    // An epoch checkpoint can carry an earlier block through an empty boundary. Preserve the
    // actual block slot; matching remote roots are only a structural claim, never finality proof.
    if finalized_epoch > current_epoch ||
        (finalized_epoch > peer.status.head_slot / config.slots_per_epoch &&
            peer.status.finalized_root != peer.status.head_root)
    {
        return Err(HandshakeError::FinalizedEpoch);
    }
    let known = if finalized_epoch == local_finalized.epoch {
        Some(local_finalized.root)
    } else {
        canonical_roots
            .iter()
            .find_map(|(epoch, root)| (*epoch == finalized_epoch).then_some(*root))
    };
    if let Some(root) = known {
        if peer.status.finalized_root != root {
            return Err(HandshakeError::FinalizedRoot);
        }
        Ok(StatusCompatibility::KnownFinality)
    } else {
        Ok(StatusCompatibility::NeedsFinalizedRoot {
            epoch: finalized_epoch,
            root: peer.status.finalized_root,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Status;

    fn peer(config: &ChainConfig, epoch: Epoch, root: Root) -> StatusV2 {
        StatusV2 {
            status: Status {
                fork_digest: config.fork_digest(2048).unwrap(),
                finalized_root: root,
                finalized_epoch: epoch,
                head_root: [8; 32],
                head_slot: 2048 * 32,
            },
            earliest_available_slot: 0,
        }
    }

    #[test]
    fn known_finalized_conflict_is_rejected_and_new_claim_stays_unverified() {
        let config = ChainConfig::hoodi();
        let checkpoint = Checkpoint { epoch: 2000, root: [2; 32] };
        let history = [(1900, [3; 32])];
        assert_eq!(
            validate_status(
                &config,
                2048 * 32,
                checkpoint,
                &history,
                &peer(&config, 1900, [3; 32])
            ),
            Ok(StatusCompatibility::KnownFinality)
        );
        assert_eq!(
            validate_status(
                &config,
                2048 * 32,
                checkpoint,
                &history,
                &peer(&config, 1900, [4; 32])
            ),
            Err(HandshakeError::FinalizedRoot)
        );
        assert_eq!(
            validate_status(
                &config,
                2048 * 32,
                checkpoint,
                &history,
                &peer(&config, 2001, [4; 32])
            ),
            Ok(StatusCompatibility::NeedsFinalizedRoot { epoch: 2001, root: [4; 32] })
        );
    }

    #[test]
    fn current_epoch_digest_is_used_even_when_peer_head_is_old() {
        let config = ChainConfig::hoodi();
        let checkpoint = Checkpoint { epoch: 2000, root: [2; 32] };
        let mut remote = peer(&config, 2000, checkpoint.root);
        remote.status.head_slot = 2047 * 32;
        assert_eq!(
            validate_status(&config, 2048 * 32, checkpoint, &[], &remote),
            Ok(StatusCompatibility::KnownFinality)
        );
        remote.status.fork_digest = config.fork_digest(2047).unwrap();
        assert_eq!(
            validate_status(&config, 2048 * 32, checkpoint, &[], &remote),
            Err(HandshakeError::ForkDigest)
        );
        remote.status.fork_digest = ChainConfig::mainnet().fork_digest(2048).unwrap();
        assert_eq!(
            validate_status(&config, 2048 * 32, checkpoint, &[], &remote),
            Err(HandshakeError::ForkDigest)
        );
    }

    #[test]
    fn inconsistent_epoch_and_local_history_fail_closed() {
        let config = ChainConfig::hoodi();
        let checkpoint = Checkpoint { epoch: 2000, root: [2; 32] };
        let mut remote = peer(&config, 2000, checkpoint.root);
        remote.status.head_slot = 1999 * 32;
        assert_eq!(
            validate_status(&config, 2048 * 32, checkpoint, &[], &remote),
            Err(HandshakeError::FinalizedEpoch)
        );
        assert_eq!(
            validate_status(&config, 2048 * 32, checkpoint, &[(2000, [4; 32])], &remote),
            Err(HandshakeError::Configuration)
        );
        remote.status = peer(&config, 2049, [4; 32]).status;
        assert_eq!(
            validate_status(&config, 2048 * 32, checkpoint, &[], &remote),
            Err(HandshakeError::FinalizedEpoch)
        );
    }
    #[test]
    fn carried_checkpoint_keeps_exact_earlier_head_slot_and_untrusted_claims_untrusted() {
        let config = ChainConfig::hoodi();
        let checkpoint = Checkpoint { epoch: 2000, root: [2; 32] };
        let mut remote = peer(&config, 2000, checkpoint.root);
        remote.status.head_root = checkpoint.root;
        remote.status.head_slot = 1999 * 32 + 31;
        assert_eq!(
            validate_status(&config, 2048 * 32, checkpoint, &[], &remote),
            Ok(StatusCompatibility::KnownFinality)
        );
        assert_eq!(remote.status.head_slot, 1999 * 32 + 31);
        remote.status.finalized_epoch = 2001;
        remote.status.finalized_root = [4; 32];
        remote.status.head_root = [4; 32];
        assert_eq!(
            validate_status(&config, 2048 * 32, checkpoint, &[], &remote),
            Ok(StatusCompatibility::NeedsFinalizedRoot { epoch: 2001, root: [4; 32] })
        );
        remote.status.head_root = [5; 32];
        assert_eq!(
            validate_status(&config, 2048 * 32, checkpoint, &[], &remote),
            Err(HandshakeError::FinalizedEpoch)
        );
        remote.status.head_root = remote.status.finalized_root;
        remote.status.finalized_epoch = 2049;
        assert_eq!(
            validate_status(&config, 2048 * 32, checkpoint, &[], &remote),
            Err(HandshakeError::FinalizedEpoch)
        );
    }
}
