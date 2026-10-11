//! Authenticated sync gossip fixtures call immutable strict production helpers.
//! The original complete anchor is retained; no execution or availability admission is inferred.

use super::{
    fixture_transition_parameters, fork_choice_fixture_path, read_snappy_ssz, schema_fork_name,
    schema_preset, yaml, Case, Outcome,
};
use reth_embedded_beacon_state_transition::{
    ssz::{
        auxiliary::altair::{SignedContributionAndProof, SyncCommitteeMessage},
        BeaconState, SignedBeaconBlock,
    },
    sync_committee_gossip::{
        self, SeenSyncCommittee, SyncCommitteeGossipError, SyncCommitteeParameters,
        SyncCommitteeState,
    },
};
use serde_yaml::Value;

pub(super) fn supports(case: &Case) -> bool {
    case.runner == "networking" &&
        case.fork != "phase0" &&
        super::FORKS.contains(&case.fork.as_str()) &&
        matches!(
            case.handler.as_str(),
            "gossip_sync_committee_message" | "gossip_sync_committee_contribution_and_proof"
        )
}

pub(super) fn run_case(case: &Case, max_bytes: usize) -> Result<(), (Outcome, String)> {
    run(case, max_bytes).map_err(|error| (Outcome::Failed, error))
}

fn run(case: &Case, max_bytes: usize) -> Result<(), String> {
    let preset = schema_preset(case)?;
    let fork = schema_fork_name(&case.fork)?;
    let metadata = yaml(&case.path.join("meta.yaml"), max_bytes)?;
    fields(&metadata, &["topic", "blocks", "messages", "bls_setting"])?;
    if !metadata["bls_setting"].is_null() && metadata["bls_setting"].as_u64() != Some(1) {
        return Err("sync committee gossip requires real fixture signatures".into())
    }
    let topic = if case.handler == "gossip_sync_committee_message" {
        "sync_committee"
    } else {
        "sync_committee_contribution_and_proof"
    };
    if metadata["topic"].as_str() != Some(topic) {
        return Err("sync fixture topic differs from handler".into())
    }
    let blocks =
        metadata["blocks"].as_sequence().ok_or("sync fixture blocks must be a sequence")?;
    if blocks.len() != 1 {
        return Err("sync adapter requires exactly one complete anchor".into())
    }
    fields(&blocks[0], &["block"])?;
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
    let header = anchor.header().map_err(|error| error.to_string())?;
    if header.slot != state.slot() ||
        header.state_root != state.hash_tree_root().map_err(|error| error.to_string())?
    {
        return Err("sync fixture complete anchor and state do not match".into())
    }
    let transition =
        fixture_transition_parameters(case, preset, fork, state.fork_info().epoch, max_bytes)?;
    let parameters = SyncCommitteeParameters::from_transition_parameters(&transition)
        .map_err(|error| error.to_string())?;
    let view = SyncCommitteeState::from_head_state(&state, &parameters)
        .map_err(|error| error.to_string())?;
    let mut seen = SeenSyncCommittee::new(&view, 262_144).map_err(|error| error.to_string())?;
    let messages =
        metadata["messages"].as_sequence().ok_or("sync fixture messages must be a sequence")?;
    if messages.is_empty() || messages.len() > 4096 {
        return Err("sync fixture message budget exceeded".into())
    }
    for (index, entry) in messages.iter().enumerate() {
        fields(entry, &["message", "current_time_ms", "subnet_id", "expected", "reason"])?;
        let time = entry["current_time_ms"].as_u64().ok_or("sync fixture time must be unsigned")?;
        let path = fork_choice_fixture_path(case, &entry["message"])?;
        let bytes = read_snappy_ssz(&path, max_bytes)?;
        let result = if topic == "sync_committee" {
            let message = SyncCommitteeMessage::from_ssz(&bytes, preset)
                .map_err(|error| error.to_string())?;
            let subnet =
                entry["subnet_id"].as_u64().ok_or("sync fixture subnet must be unsigned")?;
            sync_committee_gossip::validate_sync_committee_message(
                &mut seen, &view, &message, time, subnet,
            )
        } else {
            if !entry["subnet_id"].is_null() {
                return Err("contribution fixture must not select a subnet topic".into())
            }
            let contribution = SignedContributionAndProof::from_ssz(&bytes, preset)
                .map_err(|error| error.to_string())?;
            sync_committee_gossip::validate_sync_committee_contribution_and_proof(
                &mut seen,
                &view,
                &contribution,
                time,
            )
        };
        let (actual, reason) = match result {
            Ok(()) => ("valid", None),
            Err(SyncCommitteeGossipError::Ignore(reason)) => ("ignore", Some(reason)),
            Err(SyncCommitteeGossipError::Reject(reason)) => ("reject", Some(reason)),
            Err(error) => {
                return Err(format!("sync gossip event {index} infrastructure failure: {error}"))
            }
        };
        let expected = entry["expected"].as_str().ok_or("sync fixture result must be a string")?;
        let expected_reason = if entry["reason"].is_null() {
            None
        } else {
            Some(entry["reason"].as_str().ok_or("sync fixture reason must be a string")?)
        };
        if expected != actual || expected_reason != reason {
            return Err(format!("sync gossip event {index}: expected {expected}/{expected_reason:?}, received {actual}/{reason:?}"))
        }
    }
    Ok(())
}

fn fields(value: &Value, permitted: &[&str]) -> Result<(), String> {
    let mapping = value.as_mapping().ok_or("sync fixture metadata must be a mapping")?;
    if mapping.keys().any(|key| !key.as_str().is_some_and(|key| permitted.contains(&key))) {
        return Err("unknown sync fixture metadata field".into())
    }
    Ok(())
}
