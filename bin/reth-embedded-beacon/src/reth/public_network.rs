//! Published execution fork settings for the optional public consensus owner.

use reth_chainspec::{ChainSpec, EthereumHardfork, ForkCondition};
use reth_embedded_beacon_config::ChainConfig;
use reth_embedded_beacon_primitives::Fork;
use std::sync::Arc;

/// Add the announced Hoodi execution activation to an already checked native specification.
/// Called only by the optional public embedded owner, before any database is opened.
pub(super) fn for_consensus(
    execution: &Arc<ChainSpec>,
    consensus: &ChainConfig,
) -> eyre::Result<Arc<ChainSpec>> {
    if consensus.chain_id != 560048 {
        return Ok(Arc::clone(execution));
    }
    if consensus != &ChainConfig::hoodi() ||
        super::consensus_config_for_execution(execution)? != *consensus
    {
        eyre::bail!("published Hoodi execution settings require its exact canonical identity");
    }
    let epoch = consensus
        .forks
        .activations()
        .iter()
        .find(|activation| activation.fork == Fork::Gloas)
        .ok_or_else(|| eyre::eyre!("published Hoodi Gloas activation is missing"))?
        .epoch;
    let timestamp = epoch
        .checked_mul(consensus.slots_per_epoch)
        .and_then(|slots| slots.checked_mul(consensus.seconds_per_slot))
        .and_then(|seconds| consensus.genesis_time.checked_add(seconds))
        .ok_or_else(|| eyre::eyre!("published Hoodi execution activation overflows"))?;
    match execution.hardforks.fork(EthereumHardfork::Amsterdam) {
        ForkCondition::Never => {}
        ForkCondition::Timestamp(current) if current == timestamp => {
            return Ok(Arc::clone(execution));
        }
        _ => eyre::bail!("Hoodi Amsterdam activation conflicts with its published Gloas epoch"),
    }
    let mut configured = execution.as_ref().clone();
    configured.hardforks.insert(EthereumHardfork::Amsterdam, ForkCondition::Timestamp(timestamp));
    configured.genesis.config.amsterdam_time = Some(timestamp);
    Ok(Arc::new(configured))
}

#[cfg(test)]
mod tests {
    use super::*;
    use reth_chainspec::{EthChainSpec, HOODI, MAINNET};

    #[test]
    fn published_hoodi_upgrade_matches_beacon_clock_and_preserves_existing_settings() {
        let original = HOODI.as_ref().clone();
        let upgraded = for_consensus(&HOODI, &ChainConfig::hoodi()).unwrap();
        assert_eq!(
            upgraded.hardforks.fork(EthereumHardfork::Amsterdam),
            ForkCondition::Timestamp(1_793_036_568)
        );
        assert_eq!(upgraded.genesis.config.amsterdam_time, Some(1_793_036_568));
        assert_eq!(upgraded.genesis_hash(), original.genesis_hash());
        assert_eq!(upgraded.genesis_header, original.genesis_header);
        assert_eq!(upgraded.deposit_contract, original.deposit_contract);
        assert_eq!(upgraded.base_fee_params, original.base_fee_params);
        assert_eq!(upgraded.blob_params, original.blob_params);
        assert_eq!(
            upgraded.paris_block_and_final_difficulty,
            original.paris_block_and_final_difficulty
        );
        for fork in [
            EthereumHardfork::Shanghai,
            EthereumHardfork::Cancun,
            EthereumHardfork::Prague,
            EthereumHardfork::Osaka,
            EthereumHardfork::Bpo1,
            EthereumHardfork::Bpo2,
        ] {
            assert_eq!(upgraded.hardforks.fork(fork), original.hardforks.fork(fork));
        }
        assert_eq!(HOODI.as_ref(), &original);
    }

    #[test]
    fn public_execution_update_does_not_change_mainnet_or_admit_foreign_hoodi_specs() {
        assert!(Arc::ptr_eq(&for_consensus(&MAINNET, &ChainConfig::mainnet()).unwrap(), &MAINNET));
        let mut foreign = HOODI.as_ref().clone();
        foreign.hardforks.insert(EthereumHardfork::Amsterdam, ForkCondition::Timestamp(1));
        assert!(for_consensus(&Arc::new(foreign), &ChainConfig::hoodi()).is_err());
        let mut foreign_config = ChainConfig::hoodi();
        foreign_config.genesis_validators_root[0] ^= 1;
        assert!(for_consensus(&HOODI, &foreign_config).is_err());
    }
}
