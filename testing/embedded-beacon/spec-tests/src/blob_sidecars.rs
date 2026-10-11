//! Published Deneb/Electra blob gossip checks retain real BLS and KZG verification.
//! Declared failed-parent/finality observations stay in a sealed strict-signature fixture view.

use super::{
    config_override, fixed_hex, fixture_transition_parameters,
    fork_choice_availability::FixtureAvailability, fork_choice_fixture_path, poll_fixture_future,
    read_snappy_ssz, schema_preset, yaml, Case, FixtureExecutionEngine, Outcome,
};
use c_kzg::ethereum_kzg_settings;
use reth_embedded_beacon_fork_choice::{
    gossip_view::GossipView, store::strict_gossip_fixture::StrictGossipStore, StoreParameters,
};
use reth_embedded_beacon_node::blob_sidecars::{spec_only::SpecSeenBlobSidecars, BlobGossipError};
use reth_embedded_beacon_primitives::{Checkpoint, Fork};
use reth_embedded_beacon_state_transition::ssz::{
    auxiliary::{deneb, electra},
    BeaconState, SignedBeaconBlock,
};
use serde_yaml::Value;

pub(super) fn supports(case: &Case) -> bool {
    matches!(case.fork.as_str(), "deneb" | "electra") &&
        case.runner == "networking" &&
        case.handler == "gossip_blob_sidecar"
}
pub(super) fn uses_asserted_graph(case: &Case, max_bytes: usize) -> bool {
    supports(case) &&
        yaml(&case.path.join("meta.yaml"), max_bytes).is_ok_and(|meta| {
            !meta["finalized_checkpoint"].is_null() ||
                meta["blocks"].as_sequence().is_some_and(|blocks| {
                    blocks.iter().any(|entry| {
                        entry["failed"].as_bool() == Some(true) ||
                            entry["pending"].as_bool() == Some(true)
                    })
                })
        })
}
pub(super) fn uses_execution_mock(case: &Case, max_bytes: usize) -> bool {
    supports(case) &&
        yaml(&case.path.join("meta.yaml"), max_bytes).is_ok_and(|meta| {
            meta["blocks"].as_sequence().is_some_and(|blocks| {
                blocks.iter().skip(1).any(|entry| {
                    entry["failed"].as_bool() != Some(true) &&
                        entry["pending"].as_bool() != Some(true)
                })
            })
        })
}
pub(super) fn run_case(case: &Case, max_bytes: usize) -> Result<(), (Outcome, String)> {
    run(case, max_bytes).map_err(|error| (Outcome::Failed, error))
}

fn run(case: &Case, max_bytes: usize) -> Result<(), String> {
    let preset = schema_preset(case)?;
    let fork = match case.fork.as_str() {
        "deneb" => Fork::Deneb,
        "electra" => Fork::Electra,
        _ => return Err("blob gossip fixture fork is unsupported".into()),
    };
    let meta = yaml(&case.path.join("meta.yaml"), max_bytes)?;
    if meta["topic"].as_str() != Some("blob_sidecar") {
        return Err("blob sidecar fixture topic is invalid".into())
    }
    if !meta["bls_setting"].is_null() && meta["bls_setting"].as_u64() == Some(2) {
        return Err("blob sidecar fixtures require real signatures".into())
    }
    let blocks = meta["blocks"].as_sequence().ok_or("blob blocks must be a sequence")?;
    if blocks.is_empty() || blocks.len() > 4096 {
        return Err("blob setup block budget exceeded".into())
    }
    if declared(&blocks[0], "failed")? || declared(&blocks[0], "pending")? {
        return Err("blob fixture anchor cannot be a failed or pending observation".into())
    }
    let state = BeaconState::from_ssz(
        &read_snappy_ssz(&case.path.join("state.ssz_snappy"), max_bytes)?,
        fork,
        preset,
    )
    .map_err(|error| error.to_string())?;
    let anchor = SignedBeaconBlock::from_ssz(
        &read_snappy_ssz(&fork_choice_fixture_path(case, &blocks[0]["block"])?, max_bytes)?,
        fork,
        preset,
    )
    .map_err(|error| error.to_string())?;
    let epoch_name = if fork == Fork::Deneb { "DENEB_FORK_EPOCH" } else { "ELECTRA_FORK_EPOCH" };
    let epoch = config_override(case, epoch_name, 0, max_bytes)?;
    let processing = fixture_transition_parameters(case, preset, fork, epoch, max_bytes)?;
    let inherited = processing.gloas.fulu.electra.deneb.capella.bellatrix.altair;
    let mut parameters = StoreParameters::for_preset(preset);
    parameters.phase0 = inherited.phase0;
    parameters.altair_fork_version = inherited.fork_version;
    parameters.inactivity_score_bias = inherited.inactivity_score_bias;
    parameters.inactivity_score_recovery_rate = inherited.inactivity_score_recovery_rate;
    parameters.slot_duration_ms = processing
        .gloas
        .fulu
        .electra
        .deneb
        .capella
        .bellatrix
        .seconds_per_slot
        .checked_mul(1000)
        .ok_or("blob clock overflow")?;
    let mut store = StrictGossipStore::get_store(state, anchor.message(), parameters)
        .map_err(|error| error.to_string())?;
    store.set_transition_parameters(processing.clone()).map_err(|error| error.to_string())?;
    let engine = FixtureExecutionEngine::from_case(case, max_bytes, false)?;
    for entry in blocks.iter().skip(1) {
        let block = SignedBeaconBlock::from_ssz(
            &read_snappy_ssz(&fork_choice_fixture_path(case, &entry["block"])?, max_bytes)?,
            fork,
            preset,
        )
        .map_err(|error| error.to_string())?;
        if declared(entry, "failed")? || declared(entry, "pending")? {
            store
                .fixture_observe_block_without_state(block.message())
                .map_err(|error| error.to_string())?;
            continue
        }
        let slot = block.header().map_err(|error| error.to_string())?.slot;
        let time = slot
            .checked_mul(store.slot_duration_ms())
            .and_then(|time| store.genesis_time_ms().checked_add(time))
            .ok_or("blob setup clock overflow")?;
        store.on_tick(time.max(store.time_ms())).map_err(|error| error.to_string())?;
        let availability =
            FixtureAvailability::new(case, entry, preset, processing.gloas.fulu.clone(), max_bytes);
        poll_fixture_future(store.on_block_and_contents_with_engine(
            &block,
            &engine,
            &availability,
        ))?
        .map_err(|error| error.to_string())?;
    }
    if !meta["finalized_checkpoint"].is_null() {
        let checkpoint = &meta["finalized_checkpoint"];
        let epoch = unsigned(&checkpoint["epoch"])?;
        let root = if checkpoint["root"].is_null() {
            SignedBeaconBlock::from_ssz(
                &read_snappy_ssz(
                    &fork_choice_fixture_path(case, &checkpoint["block"])?,
                    max_bytes,
                )?,
                fork,
                preset,
            )
            .map_err(|error| error.to_string())?
            .message_root()
            .map_err(|error| error.to_string())?
        } else {
            fixed_hex::<32>(&checkpoint["root"])?
        };
        store
            .fixture_set_finalized_checkpoint(Checkpoint { epoch, root })
            .map_err(|error| error.to_string())?;
    }
    let messages = meta["messages"].as_sequence().ok_or("blob messages must be a sequence")?;
    if messages.is_empty() || messages.len() > 100_000 {
        return Err("blob message budget exceeded".into())
    }
    let mut seen = SpecSeenBlobSidecars::new(100_000, 1024).map_err(|error| error.to_string())?;
    for (index, message) in messages.iter().enumerate() {
        let bytes =
            read_snappy_ssz(&fork_choice_fixture_path(case, &message["message"])?, max_bytes)?;
        let time = unsigned(&message["current_time_ms"])?;
        let subnet = unsigned(&message["subnet_id"])?;
        let result = if fork == Fork::Deneb {
            let sidecar =
                deneb::BlobSidecar::from_ssz(&bytes, preset).map_err(|error| error.to_string())?;
            seen.validate_deneb(&store, &sidecar, time, subnet, ethereum_kzg_settings(0))
        } else {
            let sidecar = electra::BlobSidecar::from_ssz(&bytes, preset)
                .map_err(|error| error.to_string())?;
            seen.validate_electra(&store, &sidecar, time, subnet, ethereum_kzg_settings(0))
        };
        let (actual, reason) = match result {
            Ok(()) => ("valid", None),
            Err(BlobGossipError::Ignore(reason)) => ("ignore", Some(reason)),
            Err(BlobGossipError::Reject(reason)) => ("reject", Some(reason)),
            Err(error) => return Err(format!("blob message {index}: {error}")),
        };
        let expected = message["expected"].as_str().ok_or("blob outcome must be a string")?;
        if actual != expected {
            return Err(format!(
                "blob message {index}: expected {expected}, received {actual} ({reason:?})"
            ))
        }
        let expected_reason = if message["reason"].is_null() {
            None
        } else {
            Some(message["reason"].as_str().ok_or("blob reason must be a string")?)
        };
        if reason != expected_reason {
            return Err(format!(
                "blob message {index}: expected reason {expected_reason:?}, received {reason:?}"
            ))
        }
    }
    Ok(())
}
fn unsigned(value: &Value) -> Result<u64, String> {
    value.as_u64().ok_or_else(|| "blob unsigned metadata is invalid".into())
}
fn declared(value: &Value, name: &str) -> Result<bool, String> {
    if value[name].is_null() {
        Ok(false)
    } else {
        value[name].as_bool().ok_or_else(|| format!("blob {name} marker must be boolean"))
    }
}
