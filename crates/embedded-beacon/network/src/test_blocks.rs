//! Complete small SSZ blocks for wire-only tests; these grant no consensus validity.

use crate::block_rpc::WireBlock;
use reth_embedded_beacon_config::{
    ChainConfig, ForkActivation, ForkSchedule, Preset as ConfigPreset,
};
use reth_embedded_beacon_primitives::{Fork, Root};
use reth_embedded_beacon_state_transition::ssz::{phase0, List, Preset};

pub(crate) fn config() -> ChainConfig {
    let mut config = ChainConfig::hoodi();
    config.preset = ConfigPreset::Minimal;
    config.slots_per_epoch = 8;
    config.forks =
        ForkSchedule::new(vec![ForkActivation { fork: Fork::Phase0, epoch: 0, version: [0; 4] }])
            .unwrap();
    config.blob_schedule.clear();
    config
}

pub(crate) fn wire_block(slot: u64, parent_root: Root) -> WireBlock {
    let body = phase0::BeaconBlockBody {
        randao_reveal: [0; 96],
        eth1_data: phase0::Eth1Data {
            deposit_root: [0; 32],
            deposit_count: 0,
            block_hash: [0; 32],
        },
        graffiti: [0; 32],
        proposer_slashings: List::default(),
        attester_slashings: List::default(),
        attestations: List::default(),
        deposits: List::default(),
        voluntary_exits: List::default(),
    };
    let signed = phase0::SignedBeaconBlock {
        message: phase0::BeaconBlock {
            slot,
            proposer_index: 0,
            parent_root,
            state_root: [2; 32],
            body,
        },
        signature: [0; 96],
    };
    WireBlock::from_ssz(signed.to_ssz(Preset::Minimal).unwrap(), &config()).unwrap()
}
