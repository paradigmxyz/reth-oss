//! Published Gloas column gossip fixtures use strict retained block states and real KZG.

use super::{
    config_override, fixture_gloas_parameters, fork_choice_fixture_path, read_snappy_ssz,
    schema_preset, yaml, Case, Outcome,
};
use c_kzg::ethereum_kzg_settings;
use reth_embedded_beacon_fork_choice::{gloas::GloasStore, StoreParameters};
use reth_embedded_beacon_node::gloas_columns::{ColumnGossipError, SeenColumns};
use reth_embedded_beacon_state_transition::ssz::{
    auxiliary::gloas::{DataColumnSidecar, PartialDataColumnGroupID, PartialDataColumnSidecar},
    gloas::{BeaconState, SignedBeaconBlock},
};
use serde_yaml::Value;

pub(super) fn supports(case: &Case) -> bool {
    case.fork == "gloas" &&
        case.runner == "networking" &&
        matches!(
            case.handler.as_str(),
            "gossip_data_column_sidecar" | "gossip_partial_data_column_sidecar"
        )
}

pub(super) fn run_case(case: &Case, max_bytes: usize) -> Result<(), (Outcome, String)> {
    run(case, max_bytes).map_err(|error| (Outcome::Failed, error))
}

fn run(case: &Case, max_bytes: usize) -> Result<(), String> {
    let preset = schema_preset(case)?;
    let meta = yaml(&case.path.join("meta.yaml"), max_bytes)?;
    let blocks = meta["blocks"].as_sequence().ok_or("column fixture blocks must be a sequence")?;
    if blocks.is_empty() || blocks.len() > 4096 {
        return Err("column fixture block count exceeds bounded budget".into());
    }
    let state = BeaconState::from_ssz(
        &read_snappy_ssz(&case.path.join("state.ssz_snappy"), max_bytes)?,
        preset,
    )
    .map_err(|error| error.to_string())?;
    let anchor = SignedBeaconBlock::from_ssz(
        &read_snappy_ssz(&fork_choice_fixture_path(case, &blocks[0]["block"])?, max_bytes)?,
        preset,
    )
    .map_err(|error| error.to_string())?;
    if blocks[0]["failed"].as_bool() == Some(true) || blocks[0]["pending"].as_bool() == Some(true) {
        return Err("trusted fixture anchor cannot be failed or pending".into());
    }
    let mut processing = fixture_gloas_parameters(case, preset, max_bytes)?;
    processing.fork_epoch = config_override(case, "GLOAS_FORK_EPOCH", 0, max_bytes)?;
    let inherited = processing.fulu.electra.deneb.capella.bellatrix.altair;
    let mut parameters = StoreParameters::for_preset(preset);
    parameters.phase0 = inherited.phase0;
    parameters.altair_fork_version = inherited.fork_version;
    parameters.inactivity_score_bias = inherited.inactivity_score_bias;
    parameters.inactivity_score_recovery_rate = inherited.inactivity_score_recovery_rate;
    parameters.slot_duration_ms = processing
        .fulu
        .electra
        .deneb
        .capella
        .bellatrix
        .seconds_per_slot
        .checked_mul(1000)
        .ok_or("column fixture clock overflow")?;
    let mut store = GloasStore::get_store(state, anchor.message, parameters, processing)
        .map_err(|error| error.to_string())?;
    let mut seen = SeenColumns::new(4096, 131_072).map_err(|error| error.to_string())?;
    for entry in blocks.iter().skip(1) {
        let block = SignedBeaconBlock::from_ssz(
            &read_snappy_ssz(&fork_choice_fixture_path(case, &entry["block"])?, max_bytes)?,
            preset,
        )
        .map_err(|error| error.to_string())?;
        let root = block.message.hash_tree_root(preset).map_err(|error| error.to_string())?;
        seen.observe_block(root).map_err(|error| error.to_string())?;
        if entry["pending"].as_bool() == Some(true) {
            continue;
        }
        let time = store
            .genesis_time_ms()
            .checked_add(
                block
                    .message
                    .slot
                    .checked_mul(store.slot_duration_ms())
                    .ok_or("fixture block clock overflow")?,
            )
            .ok_or("fixture block clock overflow")?;
        store.on_tick(time).map_err(|error| error.to_string())?;
        let result = store.on_block_and_contents(&block);
        if entry["failed"].as_bool() == Some(true) {
            if result.is_ok() {
                return Err("fixture failed block passed strict transition".into());
            }
        } else {
            result.map_err(|error| format!("strict fixture block import: {error}"))?;
        }
    }
    let messages =
        meta["messages"].as_sequence().ok_or("column fixture messages must be a sequence")?;
    if messages.is_empty() || messages.len() > 100_000 {
        return Err("column fixture message count exceeds bounded budget".into());
    }
    let settings = ethereum_kzg_settings(0);
    for (index, message) in messages.iter().enumerate() {
        let bytes =
            read_snappy_ssz(&fork_choice_fixture_path(case, &message["message"])?, max_bytes)?;
        let actual = if case.handler == "gossip_data_column_sidecar" {
            let sidecar =
                DataColumnSidecar::from_ssz(&bytes, preset).map_err(|error| error.to_string())?;
            seen.validate_complete(
                &store,
                &sidecar,
                unsigned(&message["current_time_ms"])?,
                unsigned(&message["subnet_id"])?,
                settings,
            )
        } else {
            let sidecar = PartialDataColumnSidecar::from_ssz(&bytes, preset)
                .map_err(|error| error.to_string())?;
            let group = PartialDataColumnGroupID::from_ssz(
                &read_snappy_ssz(
                    &fork_choice_fixture_path(case, &message["group_id"])?,
                    max_bytes,
                )?,
                preset,
            )
            .map_err(|error| error.to_string())?;
            seen.validate_partial(
                &store,
                &sidecar,
                &group,
                unsigned(&message["column_index"])?,
                settings,
            )
        };
        let expected =
            message["expected"].as_str().ok_or("column fixture expected category missing")?;
        let category = match actual {
            Ok(()) => "valid",
            Err(ColumnGossipError::Ignore(_)) => "ignore",
            Err(ColumnGossipError::Reject(_)) => "reject",
            Err(error) => return Err(format!("column fixture local admission failure: {error}")),
        };
        if category != expected {
            return Err(format!("column message {index}: expected {expected}, received {category}"));
        }
    }
    Ok(())
}

fn unsigned(value: &Value) -> Result<u64, String> {
    value.as_u64().ok_or_else(|| "column fixture unsigned metadata missing".into())
}
