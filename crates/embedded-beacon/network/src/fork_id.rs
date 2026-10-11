//! Canonical discovery fork identifiers and custody integer encoding.

use reth_embedded_beacon_config::ChainConfig;
use reth_embedded_beacon_primitives::{Epoch, Fork};

/// The exact sixteen-byte SSZ `eth2` discovery value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EnrForkId {
    /// Digest for the trusted wall-clock epoch, including BPO parameters.
    pub fork_digest: [u8; 4],
    /// Fork version at the next regular or BPO activation.
    pub next_fork_version: [u8; 4],
    /// Next activation, or the far-future sentinel when none is scheduled.
    pub next_fork_epoch: Epoch,
}

/// Discovery fields derived from one verified network schedule and clock.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DiscoveryForkFields {
    /// Required `eth2` field.
    pub eth2: EnrForkId,
    /// Required `nfd` once Fulu is scheduled; zero when no later fork exists.
    pub next_fork_digest: Option<[u8; 4]>,
}

/// Remote discovery claims are advertisements, never consensus authentication.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DiscoveryCompatibility {
    /// Current and upcoming schedule advertisements agree.
    Matching,
    /// Current digest agrees, but future advertisements differ. Keep peering before the boundary.
    UpcomingForkDifference,
}

/// Malformed discovery encoding or incompatible current network.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum DiscoveryFieldError {
    /// Network configuration or genesis identity is not usable.
    #[error("invalid discovery network identity")]
    Configuration,
    /// SSZ fields or the canonical big-endian integer are malformed.
    #[error("invalid discovery field encoding")]
    Encoding,
    /// A required scheduled-Fulu advertisement is missing.
    #[error("required discovery fork field is missing")]
    Missing,
    /// The current digest belongs to another network or fork.
    #[error("incompatible current discovery fork")]
    CurrentFork,
}

impl EnrForkId {
    /// Encode the consensus-defined field without an RLP wrapper.
    pub fn to_ssz(self) -> [u8; 16] {
        let mut bytes = [0; 16];
        bytes[..4].copy_from_slice(&self.fork_digest);
        bytes[4..8].copy_from_slice(&self.next_fork_version);
        bytes[8..].copy_from_slice(&self.next_fork_epoch.to_le_bytes());
        bytes
    }

    /// Decode exactly sixteen bytes; no trailing bytes are accepted.
    pub fn from_ssz(bytes: &[u8]) -> Result<Self, DiscoveryFieldError> {
        if bytes.len() != 16 {
            return Err(DiscoveryFieldError::Encoding);
        }
        Ok(Self {
            fork_digest: bytes[..4].try_into().map_err(|_| DiscoveryFieldError::Encoding)?,
            next_fork_version: bytes[4..8].try_into().map_err(|_| DiscoveryFieldError::Encoding)?,
            next_fork_epoch: u64::from_le_bytes(
                bytes[8..].try_into().map_err(|_| DiscoveryFieldError::Encoding)?,
            ),
        })
    }
}

impl DiscoveryForkFields {
    /// Build from wall-clock epoch, requiring an authenticated nonzero genesis root.
    pub fn from_config(config: &ChainConfig, epoch: Epoch) -> Result<Self, DiscoveryFieldError> {
        config.validate().map_err(|_| DiscoveryFieldError::Configuration)?;
        if config.genesis_validators_root == [0; 32] {
            return Err(DiscoveryFieldError::Configuration);
        }
        let current =
            config.fork_at_epoch(epoch).map_err(|_| DiscoveryFieldError::Configuration)?;
        let next_epoch = config
            .forks
            .activations()
            .iter()
            .map(|fork| fork.epoch)
            .chain(config.blob_schedule.iter().map(|blob| blob.epoch))
            .filter(|next| *next > epoch && *next != u64::MAX)
            .min()
            .unwrap_or(u64::MAX);
        let next_version = if next_epoch == u64::MAX {
            current.version
        } else {
            config
                .fork_at_epoch(next_epoch)
                .map_err(|_| DiscoveryFieldError::Configuration)?
                .version
        };
        let fulu_scheduled = config
            .forks
            .activations()
            .iter()
            .any(|fork| fork.fork == Fork::Fulu && fork.epoch != u64::MAX);
        Ok(Self {
            eth2: EnrForkId {
                fork_digest: config
                    .fork_digest(epoch)
                    .map_err(|_| DiscoveryFieldError::Configuration)?,
                next_fork_version: next_version,
                next_fork_epoch: next_epoch,
            },
            next_fork_digest: if fulu_scheduled {
                Some(if next_epoch == u64::MAX {
                    [0; 4]
                } else {
                    config
                        .fork_digest(next_epoch)
                        .map_err(|_| DiscoveryFieldError::Configuration)?
                })
            } else {
                None
            },
        })
    }

    /// Check current membership while retaining peers with different upcoming digests.
    /// The remote's future epoch must be in the future or the far-future sentinel.
    pub fn compare(
        self,
        remote: Self,
        epoch: Epoch,
    ) -> Result<DiscoveryCompatibility, DiscoveryFieldError> {
        if self.eth2.fork_digest != remote.eth2.fork_digest {
            return Err(DiscoveryFieldError::CurrentFork);
        }
        if remote.eth2.next_fork_epoch != u64::MAX && remote.eth2.next_fork_epoch <= epoch {
            return Err(DiscoveryFieldError::Encoding);
        }
        if self.next_fork_digest.is_some() && remote.next_fork_digest.is_none() {
            return Err(DiscoveryFieldError::Missing);
        }
        Ok(if self == remote {
            DiscoveryCompatibility::Matching
        } else {
            DiscoveryCompatibility::UpcomingForkDifference
        })
    }
}

/// Encode `cgc` as a minimal big-endian integer. Zero is the empty byte string.
pub fn encode_custody_group_count(value: u64) -> Vec<u8> {
    let bytes = value.to_be_bytes();
    bytes[bytes.iter().position(|byte| *byte != 0).unwrap_or(8)..].to_vec()
}

/// Decode minimal `cgc`; an SSZ little-endian Uint64 is not this ENR encoding.
pub fn decode_custody_group_count(bytes: &[u8]) -> Result<u64, DiscoveryFieldError> {
    if bytes.len() > 8 || bytes.first() == Some(&0) {
        return Err(DiscoveryFieldError::Encoding);
    }
    let mut padded = [0; 8];
    padded[8 - bytes.len()..].copy_from_slice(bytes);
    Ok(u64::from_be_bytes(padded))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn literal_ssz_and_big_endian_vectors_are_distinct() {
        let value = EnrForkId {
            fork_digest: [1, 2, 3, 4],
            next_fork_version: [5, 6, 7, 8],
            next_fork_epoch: 0x0102,
        };
        let bytes = [1, 2, 3, 4, 5, 6, 7, 8, 2, 1, 0, 0, 0, 0, 0, 0];
        assert_eq!(value.to_ssz(), bytes);
        assert_eq!(EnrForkId::from_ssz(&bytes), Ok(value));
        assert_eq!(EnrForkId::from_ssz(&bytes[..15]), Err(DiscoveryFieldError::Encoding));
        assert_eq!(encode_custody_group_count(0), Vec::<u8>::new());
        assert_eq!(encode_custody_group_count(258), vec![1, 2]);
        assert_eq!(decode_custody_group_count(&[1, 2]), Ok(258));
        assert_eq!(decode_custody_group_count(&[]), Ok(0));
        assert_eq!(decode_custody_group_count(&[0]), Err(DiscoveryFieldError::Encoding));
        assert_eq!(decode_custody_group_count(&[0, 1]), Err(DiscoveryFieldError::Encoding));
        assert_eq!(decode_custody_group_count(&[1; 9]), Err(DiscoveryFieldError::Encoding));
    }

    #[test]
    fn bpo_upcoming_digest_and_disabled_fork_are_canonical() {
        let config = ChainConfig::hoodi();
        let pre_fulu = DiscoveryForkFields::from_config(&config, 2048).unwrap();
        assert_eq!(pre_fulu.eth2.next_fork_epoch, 50688);
        assert_eq!(pre_fulu.next_fork_digest, Some([0xe2, 0xab, 0xcc, 0xa4]));
        let fulu = DiscoveryForkFields::from_config(&config, 50688).unwrap();
        assert_eq!(fulu.eth2.next_fork_epoch, 52480);
        assert_eq!(fulu.eth2.next_fork_version, [0x70, 0, 9, 0x10]);
        assert_eq!(fulu.next_fork_digest, Some([0xae, 0x9f, 0x70, 0xa0]));
        let last = DiscoveryForkFields::from_config(&ChainConfig::mainnet(), 419072).unwrap();
        assert_eq!(last.eth2.next_fork_epoch, u64::MAX);
        assert_eq!(last.eth2.next_fork_version, [6, 0, 0, 0]);
        assert_eq!(last.next_fork_digest, Some([0; 4]));
    }

    #[test]
    fn upcoming_nfd_difference_does_not_reject_current_peer() {
        let local = DiscoveryForkFields::from_config(&ChainConfig::hoodi(), 50688).unwrap();
        let mut remote = local;
        remote.next_fork_digest = Some([9; 4]);
        assert_eq!(
            local.compare(remote, 50688),
            Ok(DiscoveryCompatibility::UpcomingForkDifference)
        );
        remote.eth2.fork_digest = [9; 4];
        assert_eq!(local.compare(remote, 50688), Err(DiscoveryFieldError::CurrentFork));
        remote = local;
        remote.next_fork_digest = None;
        assert_eq!(local.compare(remote, 50688), Err(DiscoveryFieldError::Missing));
        remote = local;
        remote.eth2.next_fork_epoch = 50688;
        assert_eq!(local.compare(remote, 50688), Err(DiscoveryFieldError::Encoding));
    }
}
