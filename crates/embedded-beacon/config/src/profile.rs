//! Resource and shared-retention policy independent of the consensus preset.

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Effective embedded resource policy; public chains keep their authenticated consensus preset.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EmbeddedProfile {
    /// Whether the existing execution minimal-storage flag selected this profile.
    pub minimal: bool,
    /// Cache capacity for the single shared immutable object store.
    pub shared_cache_bytes: usize,
    /// Hot decoded consensus-state ownership; independent of the immutable-byte cache.
    pub state_cache_bytes: usize,
    /// Optional execution RPC block-cache ceiling; native defaults remain unchanged otherwise.
    pub rpc_block_cache_bytes: Option<usize>,
    /// Optional execution RPC receipt-cache ceiling.
    pub rpc_receipt_cache_bytes: Option<usize>,
    /// Optional execution RPC block-access-list-cache ceiling.
    pub rpc_bal_cache_bytes: Option<usize>,
    /// Recent consensus state slots eligible for the hot cache, never a pruning boundary.
    pub hot_state_slots: u64,
    /// Distance between complete finalized snapshots for a separately wired verified diff store.
    pub finalized_snapshot_interval_epochs: u64,
    /// Maximum reconstruction distance permitted by that snapshot policy.
    pub max_state_replay_epochs: u64,
    /// Every unfinalized state must remain reconstructible regardless of the hot-cache window.
    pub retain_all_unfinalized: bool,
    /// EL and CL independently pin shared objects before either layer may release ownership.
    pub separate_el_cl_pins: bool,
}

impl EmbeddedProfile {
    /// Resolve additive defaults and a caller's explicit shared-cache override.
    pub fn resolve(minimal: bool, cache_mib: Option<usize>) -> Result<Self, ProfileError> {
        let cache_mib = cache_mib.unwrap_or(if minimal { 64 } else { 256 });
        if cache_mib == 0 || cache_mib > 4096 {
            return Err(ProfileError::InvalidSharedCache)
        }
        let shared_cache_bytes =
            cache_mib.checked_mul(1024 * 1024).ok_or(ProfileError::InvalidSharedCache)?;
        Ok(Self {
            minimal,
            shared_cache_bytes,
            state_cache_bytes: if minimal { 64 } else { 256 } * 1024 * 1024,
            rpc_block_cache_bytes: minimal.then_some(16 * 1024 * 1024),
            rpc_receipt_cache_bytes: minimal.then_some(8 * 1024 * 1024),
            rpc_bal_cache_bytes: minimal.then_some(8 * 1024 * 1024),
            hot_state_slots: if minimal { 64 } else { 128 },
            finalized_snapshot_interval_epochs: if minimal { 256 } else { 64 },
            max_state_replay_epochs: if minimal { 256 } else { 64 },
            retain_all_unfinalized: true,
            separate_el_cl_pins: true,
        })
    }

    /// Override decoded-state caching without changing retention or consensus parameters.
    pub fn with_state_cache_mib(mut self, cache_mib: Option<usize>) -> Result<Self, ProfileError> {
        if let Some(mib) = cache_mib {
            if !(1..=2048).contains(&mib) {
                return Err(ProfileError::InvalidStateCache)
            }
            self.state_cache_bytes =
                mib.checked_mul(1024 * 1024).ok_or(ProfileError::InvalidStateCache)?;
        }
        Ok(self)
    }
}

/// Invalid explicit resource policy.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum ProfileError {
    /// Cache capacity must be finite, positive and representable in bytes.
    #[error("shared cache must be between 1 and 4096 MiB")]
    InvalidSharedCache,
    /// Decoded cache must fit the complete-state resident allocation limit.
    #[error("state cache must be between 1 and 2048 MiB")]
    InvalidStateCache,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn minimal_profile_bounds_caches_without_shortening_consensus_ownership() {
        let standard = EmbeddedProfile::resolve(false, None).unwrap();
        assert_eq!(standard.shared_cache_bytes, 256 * 1024 * 1024);
        assert_eq!(standard.rpc_block_cache_bytes, None);
        let minimal = EmbeddedProfile::resolve(true, None).unwrap();
        assert_eq!(minimal.shared_cache_bytes, 64 * 1024 * 1024);
        assert_eq!(minimal.state_cache_bytes, 64 * 1024 * 1024);
        assert_eq!(standard.state_cache_bytes, 256 * 1024 * 1024);
        let tuned = minimal.clone().with_state_cache_mib(Some(512)).unwrap();
        assert_eq!(tuned.state_cache_bytes, 512 * 1024 * 1024);
        assert_eq!(tuned.shared_cache_bytes, minimal.shared_cache_bytes);
        assert!(tuned.retain_all_unfinalized && tuned.separate_el_cl_pins);
        for mib in [0, 2049, usize::MAX] {
            assert_eq!(
                minimal.clone().with_state_cache_mib(Some(mib)),
                Err(ProfileError::InvalidStateCache)
            );
        }
        assert_eq!(minimal.rpc_block_cache_bytes, Some(16 * 1024 * 1024));
        assert!(minimal.retain_all_unfinalized && minimal.separate_el_cl_pins);
        assert!(minimal.finalized_snapshot_interval_epochs <= minimal.max_state_replay_epochs);
        let explicit = EmbeddedProfile::resolve(true, Some(128)).unwrap();
        assert_eq!(explicit.shared_cache_bytes, 128 * 1024 * 1024);
        assert_eq!(EmbeddedProfile::resolve(true, Some(0)), Err(ProfileError::InvalidSharedCache));
        assert_eq!(
            EmbeddedProfile::resolve(false, Some(usize::MAX)),
            Err(ProfileError::InvalidSharedCache)
        );
    }
}
