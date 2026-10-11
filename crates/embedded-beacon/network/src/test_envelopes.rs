//! Complete Gloas signed wire objects for transport tests; these grant no consensus validity.

use crate::{block_rpc::WireBlock, envelope_rpc::WireEnvelope};
use reth_embedded_beacon_config::{ChainConfig, ForkSchedule, Preset as ConfigPreset};
use reth_embedded_beacon_primitives::Root;
use reth_embedded_beacon_state_transition::ssz::{gloas, Preset, Ssz};

pub(crate) fn config() -> ChainConfig {
    let mut config = ChainConfig::hoodi();
    config.preset = ConfigPreset::Minimal;
    config.slots_per_epoch = 8;
    let activations = config
        .forks
        .activations()
        .iter()
        .copied()
        .map(|mut activation| {
            activation.epoch = 0;
            activation
        })
        .collect();
    config.forks = ForkSchedule::new(activations).unwrap();
    config.blob_schedule.clear();
    config.validate().unwrap();
    config
}

pub(crate) fn wire_block(slot: u64, parent_root: Root) -> WireBlock {
    let mut signed = gloas::SignedBeaconBlock::default_value(Preset::Minimal).unwrap();
    signed.message.slot = slot;
    signed.message.parent_root = parent_root;
    signed.message.state_root = [2; 32];
    signed.message.body.signed_execution_payload_bid.message.slot = slot;
    signed.message.body.signed_execution_payload_bid.message.parent_block_root = parent_root;
    WireBlock::from_ssz(signed.to_ssz(Preset::Minimal).unwrap(), &config()).unwrap()
}

pub(crate) fn signed_envelope(block: &WireBlock) -> gloas::SignedExecutionPayloadEnvelope {
    let mut signed = gloas::SignedExecutionPayloadEnvelope::default_value(Preset::Minimal).unwrap();
    signed.message.beacon_block_root = block.root();
    signed.message.parent_beacon_block_root = block.parent_root();
    signed.message.payload.slot_number = block.slot();
    // Deliberately unrelated to beacon time: wire context must never be inferred from this field.
    signed.message.payload.timestamp = u64::MAX;
    signed
}

pub(crate) fn wire_envelope(block: &WireBlock) -> WireEnvelope {
    WireEnvelope::from_ssz(
        signed_envelope(block).to_ssz(Preset::Minimal).unwrap(),
        block,
        &config(),
    )
    .unwrap()
}
