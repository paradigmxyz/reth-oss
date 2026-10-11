//! Authenticated Gloas bid/preferences fixtures exercise shared strict-signature gossip logic.
//! Declared checkpoint, activation and FULL observations stay in an isolated graph. Envelopes
//! use real gossip signatures and propagation checks; no execution or availability is credited.

use super::{
    config_override, fixture_gloas_parameters, fork_choice_fixture_path, read_snappy_ssz,
    schema_preset, yaml, Case, Outcome,
};
use reth_embedded_beacon_fork_choice::{
    gloas::{gossip_view::GloasGossipView, spec_only::SpecStore},
    StoreParameters,
};
use reth_embedded_beacon_node::gloas_bids::{spec_only::SpecSeenBids, BidGossipError};
use reth_embedded_beacon_primitives::Checkpoint;
use reth_embedded_beacon_state_transition::ssz::{
    auxiliary::gloas::SignedProposerPreferences,
    gloas::{
        BeaconState, SignedBeaconBlock, SignedExecutionPayloadBid, SignedExecutionPayloadEnvelope,
    },
};
use serde_yaml::Value;
use std::collections::{BTreeMap, BTreeSet};

pub(super) fn supports(case: &Case) -> bool {
    case.fork == "gloas" &&
        case.runner == "networking" &&
        matches!(
            case.handler.as_str(),
            "gossip_execution_payload_bid" | "gossip_proposer_preferences"
        )
}

pub(super) fn uses_spec_store(case: &Case, _max_bytes: usize) -> bool {
    supports(case)
}

pub(super) fn uses_asserted_graph(case: &Case, max_bytes: usize) -> bool {
    supports(case) &&
        (config_override(case, "GLOAS_FORK_EPOCH", 0, max_bytes).is_ok_and(|epoch| epoch != 0) ||
            yaml(&case.path.join("meta.yaml"), max_bytes).is_ok_and(|meta| {
                !meta["finalized_checkpoint"].is_null() ||
                    meta["blocks"].as_sequence().is_some_and(|blocks| {
                        blocks.len() > 1 ||
                            blocks.iter().any(|block| {
                                block["pending"].as_bool() == Some(true) ||
                                    !block["payload"].is_null()
                            })
                    }) ||
                    meta["messages"].as_sequence().is_some_and(|messages| {
                        messages.iter().any(|message| {
                            message["message"]
                                .as_str()
                                .is_some_and(|name| name.starts_with("execution_payload_envelope_"))
                        })
                    })
            }))
}

// These are envelope PROPAGATION fixtures. They do not invoke Engine/DA callbacks; declared FULL
// branches are reported as asserted graph context, without crediting execution or availability.
pub(super) const fn uses_execution_mock(_case: &Case, _max_bytes: usize) -> bool {
    false
}

pub(super) fn run_case(case: &Case, max_bytes: usize) -> Result<(), (Outcome, String)> {
    run(case, max_bytes).map_err(|error| (Outcome::Failed, error))
}

fn run(case: &Case, max_bytes: usize) -> Result<(), String> {
    let preset = schema_preset(case)?;
    let meta = yaml(&case.path.join("meta.yaml"), max_bytes)?;
    fields(&meta, &["topic", "blocks", "messages", "finalized_checkpoint"])?;
    let expected_topic = if case.handler == "gossip_execution_payload_bid" {
        "execution_payload_bid"
    } else {
        "proposer_preferences"
    };
    if meta["topic"].as_str() != Some(expected_topic) {
        return Err("bid/preference fixture topic differs from its handler".into())
    }
    let blocks = meta["blocks"].as_sequence().ok_or("bid fixture blocks must be a sequence")?;
    if blocks.is_empty() || blocks.len() > 4096 {
        return Err("bid fixture block count exceeds bounded budget".into())
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
    if !blocks[0]["pending"].is_null() {
        return Err("fixture anchor cannot declare a pending observation".into())
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
        .ok_or("bid fixture clock overflow")?;
    macro_rules! setting {
        ($field:ident, $name:literal) => {
            parameters.$field = config_override(case, $name, parameters.$field, max_bytes)?;
        };
    }
    setting!(gloas_attestation_due_bps, "ATTESTATION_DUE_BPS_GLOAS");
    setting!(payload_attestation_due_bps, "PAYLOAD_ATTESTATION_DUE_BPS");
    setting!(proposer_score_boost, "PROPOSER_SCORE_BOOST");
    let root = anchor.message.hash_tree_root(preset).map_err(|error| error.to_string())?;
    let mut known = BTreeMap::from([(
        blocks[0]["block"].as_str().ok_or("anchor identifier missing")?.to_owned(),
        root,
    )]);
    let mut store = SpecStore::get_store(state, anchor.message, parameters, processing)
        .map_err(|error| error.to_string())?;
    let mut seen = SpecSeenBids::new(131_072, 4096).map_err(|error| error.to_string())?;
    for (index, entry) in blocks.iter().enumerate() {
        fields(entry, &["block", "pending", "payload"])?;
        if index != 0 {
            let block = SignedBeaconBlock::from_ssz(
                &read_snappy_ssz(&fork_choice_fixture_path(case, &entry["block"])?, max_bytes)?,
                preset,
            )
            .map_err(|error| error.to_string())?;
            let root = block.message.hash_tree_root(preset).map_err(|error| error.to_string())?;
            let identifier = entry["block"].as_str().ok_or("block identifier missing")?;
            if known.insert(identifier.to_owned(), root).is_some() {
                return Err("duplicate fixture block identifier".into())
            }
            seen.observe_block(&block.message, preset).map_err(|error| error.to_string())?;
            if !entry["pending"].is_null() && entry["pending"].as_bool().is_none() {
                return Err("fixture pending observation must be boolean".into())
            }
            if entry["pending"].as_bool() == Some(true) {
                if !entry["payload"].is_null() {
                    return Err("pending block cannot publish an execution envelope".into())
                }
                continue
            }
            let time = block
                .message
                .slot
                .checked_mul(store.slot_duration_ms())
                .and_then(|time| store.genesis_time_ms().checked_add(time))
                .ok_or("fixture block clock overflow")?;
            store.on_tick(time.max(store.time_ms())).map_err(|error| error.to_string())?;
            store
                .on_block_and_contents(&block)
                .map_err(|error| format!("strict bid fixture block import: {error}"))?;
        }
        if !entry["payload"].is_null() {
            let envelope = read_envelope(case, max_bytes, &entry["payload"])?;
            store
                .fixture_declare_gossip_payload(&envelope)
                .map_err(|error| format!("asserted fixture envelope gossip: {error}"))?;
        }
    }
    let messages =
        meta["messages"].as_sequence().ok_or("bid fixture messages must be a sequence")?;
    if messages.is_empty() || messages.len() > 100_000 {
        return Err("bid fixture message count exceeds bounded budget".into())
    }
    if !meta["finalized_checkpoint"].is_null() {
        let checkpoint = &meta["finalized_checkpoint"];
        fields(checkpoint, &["epoch", "block"])?;
        let identifier =
            checkpoint["block"].as_str().ok_or("finalized block identifier missing")?;
        let root = known.get(identifier).copied().ok_or("finalized fixture block not observed")?;
        let mut replay_roots =
            BTreeSet::from([store.get_head().map_err(|error| error.to_string())?.root]);
        for message in messages {
            if message["message"]
                .as_str()
                .is_some_and(|name| name.starts_with("execution_payload_bid_"))
            {
                let bid = SignedExecutionPayloadBid::from_ssz(
                    &read_snappy_ssz(
                        &fork_choice_fixture_path(case, &message["message"])?,
                        max_bytes,
                    )?,
                    preset,
                )
                .map_err(|error| error.to_string())?;
                if store.block_state(bid.message.parent_block_root).is_some() {
                    replay_roots.insert(bid.message.parent_block_root);
                }
            }
        }
        store
            .fixture_set_finalized_checkpoint_for_states(
                Checkpoint { epoch: unsigned(&checkpoint["epoch"])?, root },
                &replay_roots.into_iter().collect::<Vec<_>>(),
            )
            .map_err(|error| error.to_string())?;
    }
    let mut admissions = 0usize;
    for (index, message) in messages.iter().enumerate() {
        fields(message, &["current_time_ms", "message", "expected", "reason"])?;
        let name = message["message"].as_str().ok_or("bid fixture message identifier missing")?;
        if name.starts_with("execution_payload_envelope_") {
            let envelope = read_envelope(case, max_bytes, &message["message"])?;
            compare_result(seen.validate_envelope(&store, &envelope), message, index)?;
            continue
        }
        let bytes =
            read_snappy_ssz(&fork_choice_fixture_path(case, &message["message"])?, max_bytes)?;
        let time = unsigned(&message["current_time_ms"])?;
        let actual = if name.starts_with("execution_payload_bid_") {
            let bid = SignedExecutionPayloadBid::from_ssz(&bytes, preset)
                .map_err(|error| error.to_string())?;
            seen.validate_bid(&store, &bid, time)
        } else if name.starts_with("proposer_preferences_") {
            let preferences = SignedProposerPreferences::from_ssz(&bytes, preset)
                .map_err(|error| error.to_string())?;
            seen.validate_preferences(&store, &preferences, time)
        } else {
            return Err("unsupported bid fixture message type".into())
        };
        compare_result(actual, message, index)?;
        admissions = admissions.checked_add(1).ok_or("bid fixture admission count overflow")?;
    }
    if admissions == 0 {
        return Err("bid fixture has no checked gossip admission".into())
    }
    Ok(())
}

fn read_envelope(
    case: &Case,
    max_bytes: usize,
    identifier: &Value,
) -> Result<SignedExecutionPayloadEnvelope, String> {
    SignedExecutionPayloadEnvelope::from_ssz(
        &read_snappy_ssz(&fork_choice_fixture_path(case, identifier)?, max_bytes)?,
        schema_preset(case)?,
    )
    .map_err(|error| error.to_string())
}

fn compare_result(
    actual: Result<(), BidGossipError>,
    message: &Value,
    index: usize,
) -> Result<(), String> {
    let (category, reason) = match actual {
        Ok(()) => ("valid", None),
        Err(BidGossipError::Ignore(reason)) => ("ignore", Some(reason)),
        Err(BidGossipError::Reject(reason)) => ("reject", Some(reason)),
        Err(error) => {
            return Err(format!("bid fixture infrastructure failure at message {index}: {error}"))
        }
    };
    let expected = message["expected"].as_str().ok_or("expected bid category missing")?;
    if category != expected || reason != message["reason"].as_str() {
        return Err(format!("bid admission message {index}: expected {expected} {:?}, received {category} {reason:?}", message["reason"].as_str()))
    }
    Ok(())
}

fn unsigned(value: &Value) -> Result<u64, String> {
    value
        .as_u64()
        .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
        .ok_or_else(|| "invalid unsigned bid metadata".into())
}

fn fields(value: &Value, allowed: &[&str]) -> Result<(), String> {
    let mapping = value.as_mapping().ok_or("bid fixture context must be a mapping")?;
    if mapping.keys().any(|key| !key.as_str().is_some_and(|key| allowed.contains(&key))) {
        return Err("unsupported bid fixture context field".into())
    }
    Ok(())
}
