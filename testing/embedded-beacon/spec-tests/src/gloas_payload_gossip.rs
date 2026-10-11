//! Published Gloas envelope and PTC propagation use real BLS and the production ordered kernels.
//! Declared failed observations and finalized gossip timing stay in sealed fixture contexts;
//! accepting an envelope or PTC message invokes no Engine/DA callback and grants no FULL branch.

use super::{
    config_override, fixture_gloas_parameters, fork_choice_fixture_path, read_snappy_ssz,
    schema_preset, yaml, Case, Outcome,
};
use reth_embedded_beacon_fork_choice::{gloas::spec_only::SpecStore, StoreParameters};
use reth_embedded_beacon_node::{
    gloas_bids::{spec_only::SpecSeenBids, BidGossipError},
    gloas_payload_attestations::{
        spec_only::SpecSeenPayloadAttestations, PayloadAttestationGossipError,
    },
};
use reth_embedded_beacon_primitives::{root_from_hex, Checkpoint};
use reth_embedded_beacon_state_transition::ssz::{
    auxiliary::gloas::PayloadAttestationMessage,
    gloas::{BeaconState, SignedBeaconBlock, SignedExecutionPayloadEnvelope},
};
use serde_yaml::Value;

pub(super) fn supports(case: &Case) -> bool {
    case.fork == "gloas" &&
        case.runner == "networking" &&
        matches!(
            case.handler.as_str(),
            "gossip_execution_payload_envelope" | "gossip_payload_attestation_message"
        )
}

pub(super) fn uses_asserted_graph(case: &Case, max_bytes: usize) -> bool {
    supports(case) &&
        (config_override(case, "GLOAS_FORK_EPOCH", 0, max_bytes).is_ok_and(|epoch| epoch != 0) ||
            yaml(&case.path.join("meta.yaml"), max_bytes).is_ok_and(|meta| {
                !meta["finalized_checkpoint"].is_null() ||
                    meta["blocks"].as_sequence().is_some_and(|blocks| {
                        blocks.iter().any(|block| {
                            block["failed"].as_bool() == Some(true) ||
                                block["pending"].as_bool() == Some(true)
                        })
                    })
            }))
}

pub(super) fn run_case(case: &Case, max_bytes: usize) -> Result<(), (Outcome, String)> {
    run(case, max_bytes).map_err(|error| (Outcome::Failed, error))
}

fn run(case: &Case, max_bytes: usize) -> Result<(), String> {
    let preset = schema_preset(case)?;
    let meta = yaml(&case.path.join("meta.yaml"), max_bytes)?;
    fields(&meta, &["topic", "blocks", "messages", "finalized_checkpoint"])?;
    let envelope_topic = case.handler == "gossip_execution_payload_envelope";
    let topic = if envelope_topic { "execution_payload" } else { "payload_attestation_message" };
    if meta["topic"].as_str() != Some(topic) {
        return Err("payload-gossip topic differs from its handler".into())
    }
    let blocks = meta["blocks"].as_sequence().ok_or("payload-gossip blocks must be a sequence")?;
    if blocks.is_empty() || blocks.len() > 4096 {
        return Err("payload-gossip block count exceeds bounded budget".into())
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
        return Err("payload-gossip anchor cannot be failed or pending".into())
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
        .ok_or("payload-gossip clock overflow")?;
    parameters.gloas_attestation_due_bps = config_override(
        case,
        "ATTESTATION_DUE_BPS_GLOAS",
        parameters.gloas_attestation_due_bps,
        max_bytes,
    )?;
    parameters.payload_attestation_due_bps = config_override(
        case,
        "PAYLOAD_ATTESTATION_DUE_BPS",
        parameters.payload_attestation_due_bps,
        max_bytes,
    )?;
    parameters.proposer_score_boost =
        config_override(case, "PROPOSER_SCORE_BOOST", parameters.proposer_score_boost, max_bytes)?;
    let mut store = SpecStore::get_store(state, anchor.message, parameters, processing)
        .map_err(|error| error.to_string())?;
    let mut seen_envelopes = SpecSeenBids::new(131_072, 4096).map_err(|error| error.to_string())?;
    let mut seen_attestations =
        SpecSeenPayloadAttestations::new(131_072).map_err(|error| error.to_string())?;
    for (index, entry) in blocks.iter().enumerate() {
        fields(entry, &["block", "failed", "pending"])?;
        for flag in ["failed", "pending"] {
            if !entry[flag].is_null() && entry[flag].as_bool().is_none() {
                return Err("payload-gossip block observation must be boolean".into())
            }
        }
        if index == 0 {
            continue
        }
        let block = SignedBeaconBlock::from_ssz(
            &read_snappy_ssz(&fork_choice_fixture_path(case, &entry["block"])?, max_bytes)?,
            preset,
        )
        .map_err(|error| error.to_string())?;
        seen_envelopes.observe_block(&block.message, preset).map_err(|error| error.to_string())?;
        seen_attestations
            .observe_block(&block.message, preset)
            .map_err(|error| error.to_string())?;
        if entry["failed"].as_bool() == Some(true) || entry["pending"].as_bool() == Some(true) {
            continue
        }
        let time = block
            .message
            .slot
            .checked_mul(store.slot_duration_ms())
            .and_then(|time| store.genesis_time_ms().checked_add(time))
            .ok_or("payload-gossip block clock overflow")?;
        store.on_tick(time.max(store.time_ms())).map_err(|error| error.to_string())?;
        store
            .on_block_and_contents(&block)
            .map_err(|error| format!("strict payload-gossip block import: {error}"))?;
    }
    if !meta["finalized_checkpoint"].is_null() {
        let checkpoint = &meta["finalized_checkpoint"];
        fields(checkpoint, &["epoch", "root"])?;
        let root =
            root_from_hex(checkpoint["root"].as_str().ok_or("gossip finalized root missing")?)
                .map_err(|error| error.to_string())?;
        store
            .fixture_set_gossip_finalized_checkpoint(Checkpoint {
                epoch: unsigned(&checkpoint["epoch"])?,
                root,
            })
            .map_err(|error| error.to_string())?;
    }
    let messages =
        meta["messages"].as_sequence().ok_or("payload-gossip messages must be a sequence")?;
    if messages.is_empty() || messages.len() > 100_000 {
        return Err("payload-gossip message count exceeds bounded budget".into())
    }
    for (index, message) in messages.iter().enumerate() {
        fields(message, &["current_time_ms", "message", "expected", "reason"])?;
        let identifier =
            message["message"].as_str().ok_or("payload-gossip message identifier missing")?;
        let bytes =
            read_snappy_ssz(&fork_choice_fixture_path(case, &message["message"])?, max_bytes)?;
        let actual = if envelope_topic {
            if !identifier.starts_with("execution_payload_envelope_") {
                return Err("envelope fixture contains another message type".into())
            }
            let envelope = SignedExecutionPayloadEnvelope::from_ssz(&bytes, preset)
                .map_err(|error| error.to_string())?;
            envelope_outcome(seen_envelopes.validate_envelope(&store, &envelope))?
        } else {
            if !identifier.starts_with("payload_attestation_message_") {
                return Err("PTC fixture contains another message type".into())
            }
            let value = PayloadAttestationMessage::from_ssz(&bytes, preset)
                .map_err(|error| error.to_string())?;
            attestation_outcome(seen_attestations.validate_message(
                &store,
                &value,
                unsigned(&message["current_time_ms"])?,
            ))?
        };
        let expected =
            message["expected"].as_str().ok_or("expected payload-gossip category missing")?;
        if actual.0 != expected || actual.1 != message["reason"].as_str() {
            return Err(format!(
                "payload-gossip message {index}: expected {expected} {:?}, received {} {:?}",
                message["reason"].as_str(),
                actual.0,
                actual.1
            ))
        }
    }
    Ok(())
}

fn envelope_outcome(
    actual: Result<(), BidGossipError>,
) -> Result<(&'static str, Option<&'static str>), String> {
    match actual {
        Ok(()) => Ok(("valid", None)),
        Err(BidGossipError::Ignore(reason)) => Ok(("ignore", Some(reason))),
        Err(BidGossipError::Reject(reason)) => Ok(("reject", Some(reason))),
        Err(error) => Err(format!("envelope gossip infrastructure failure: {error}")),
    }
}
fn attestation_outcome(
    actual: Result<(), PayloadAttestationGossipError>,
) -> Result<(&'static str, Option<&'static str>), String> {
    match actual {
        Ok(()) => Ok(("valid", None)),
        Err(PayloadAttestationGossipError::Ignore(reason)) => Ok(("ignore", Some(reason))),
        Err(PayloadAttestationGossipError::Reject(reason)) => Ok(("reject", Some(reason))),
        Err(error) => Err(format!("PTC gossip infrastructure failure: {error}")),
    }
}
fn unsigned(value: &Value) -> Result<u64, String> {
    value
        .as_u64()
        .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
        .ok_or_else(|| "invalid unsigned payload-gossip metadata".into())
}
fn fields(value: &Value, allowed: &[&str]) -> Result<(), String> {
    let mapping = value.as_mapping().ok_or("payload-gossip context must be a mapping")?;
    if mapping.keys().any(|key| !key.as_str().is_some_and(|key| allowed.contains(&key))) {
        return Err("unsupported payload-gossip context field".into())
    }
    Ok(())
}
