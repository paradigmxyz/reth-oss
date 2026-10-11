//! Authenticated operation gossip fixtures exercise immutable mandatory-signature helpers.
//! Their single complete anchor is checked directly; no block, Engine or DA admission is inferred.

use super::{
    config_override, fixture_transition_parameters, fork_choice_fixture_path, read_snappy_ssz,
    schema_fork_name, schema_preset, yaml, Case, Outcome,
};
use reth_embedded_beacon_primitives::Fork;
use reth_embedded_beacon_state_transition::{
    gossip_operations::{
        spec_only::SpecOperationParameters, IndexedAttestationRef, OperationGossipError,
        OperationParameters, OperationState, SeenOperations,
    },
    ssz::{
        altair, bellatrix, capella, deneb, electra, fulu, gloas, phase0, BeaconState,
        SignedBeaconBlock, Ssz,
    },
};
use serde_yaml::Value;

pub(super) fn supports(case: &Case) -> bool {
    case.runner == "networking" &&
        matches!(
            case.handler.as_str(),
            "gossip_voluntary_exit" |
                "gossip_proposer_slashing" |
                "gossip_attester_slashing" |
                "gossip_bls_to_execution_change"
        )
}

pub(super) fn uses_asserted_graph(case: &Case, max_bytes: usize) -> bool {
    supports(case) &&
        case.handler == "gossip_bls_to_execution_change" &&
        config_override(case, "CAPELLA_FORK_EPOCH", 0, max_bytes).is_ok_and(|epoch| epoch != 0)
}

pub(super) fn run_case(case: &Case, max_bytes: usize) -> Result<(), (Outcome, String)> {
    run(case, max_bytes).map_err(|error| (Outcome::Failed, error))
}

fn run(case: &Case, max_bytes: usize) -> Result<(), String> {
    let preset = schema_preset(case)?;
    let fork = schema_fork_name(&case.fork)?;
    let meta = yaml(&case.path.join("meta.yaml"), max_bytes)?;
    fields(&meta, &["topic", "blocks", "messages", "bls_setting"])?;
    if !meta["bls_setting"].is_null() && meta["bls_setting"].as_u64() != Some(1) {
        return Err("operation gossip requires real fixture signatures".into());
    }
    let topic = case.handler.strip_prefix("gossip_").ok_or("operation handler topic missing")?;
    if meta["topic"].as_str() != Some(topic) {
        return Err("operation fixture topic differs from handler".into());
    }
    let blocks =
        meta["blocks"].as_sequence().ok_or("operation fixture blocks must be a sequence")?;
    if blocks.len() != 1 {
        return Err("operation adapter requires exactly one complete anchor".into());
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
        return Err("operation fixture complete anchor and state do not match".into());
    }
    let transition =
        fixture_transition_parameters(case, preset, fork, state.fork_info().epoch, max_bytes)?;
    let capella_epoch =
        config_override(case, "CAPELLA_FORK_EPOCH", transition.schedule[3].epoch, max_bytes)?;
    let parameters = SpecOperationParameters::new(
        OperationParameters::from_transition_parameters(&transition)
            .map_err(|error| error.to_string())?,
        capella_epoch,
    );
    let mut seen = SeenOperations::new(262_144).map_err(|error| error.to_string())?;
    let messages =
        meta["messages"].as_sequence().ok_or("operation fixture messages must be a sequence")?;
    if messages.is_empty() || messages.len() > 4096 {
        return Err("operation fixture message budget exceeded".into());
    }
    let view = OperationState::from_state(&state);
    for (index, entry) in messages.iter().enumerate() {
        fields(entry, &["message", "current_time_ms", "expected", "reason"])?;
        let time = if entry["current_time_ms"].is_null() {
            0
        } else {
            entry["current_time_ms"].as_u64().ok_or("operation gossip time must be unsigned")?
        };
        let result = match topic {
            "voluntary_exit" => parameters.voluntary_exit(
                &mut seen,
                view,
                &message::<phase0::SignedVoluntaryExit>(case, &entry["message"], max_bytes)?,
                time,
            ),
            "proposer_slashing" => parameters.proposer_slashing(
                &mut seen,
                view,
                &message::<phase0::ProposerSlashing>(case, &entry["message"], max_bytes)?,
            ),
            "bls_to_execution_change" => {
                // This fixed container has identical field layout and roots in every supported
                // fork.
                parameters.credential_change(
                    &mut seen,
                    view,
                    &message::<capella::SignedBLSToExecutionChange>(
                        case,
                        &entry["message"],
                        max_bytes,
                    )?,
                    time,
                )
            }
            "attester_slashing" => {
                macro_rules! validate {
                    ($module:ident) => {{
                        let operation = message::<$module::AttesterSlashing>(
                            case,
                            &entry["message"],
                            max_bytes,
                        )?;
                        parameters.attester_slashing(
                            &mut seen,
                            view,
                            IndexedAttestationRef {
                                indices: &operation.attestation_1.attesting_indices.values,
                                data: &operation.attestation_1.data,
                                signature: &operation.attestation_1.signature,
                            },
                            IndexedAttestationRef {
                                indices: &operation.attestation_2.attesting_indices.values,
                                data: &operation.attestation_2.data,
                                signature: &operation.attestation_2.signature,
                            },
                        )
                    }};
                }
                match fork {
                    Fork::Phase0 => validate!(phase0),
                    Fork::Altair => validate!(altair),
                    Fork::Bellatrix => validate!(bellatrix),
                    Fork::Capella => validate!(capella),
                    Fork::Deneb => validate!(deneb),
                    Fork::Electra => validate!(electra),
                    Fork::Fulu => validate!(fulu),
                    Fork::Gloas => validate!(gloas),
                }
            }
            _ => return Err("unknown operation gossip handler".into()),
        };
        compare(entry, result)
            .map_err(|error| format!("operation gossip message {index}: {error}"))?;
    }
    Ok(())
}

fn message<T: Ssz>(case: &Case, name: &Value, max_bytes: usize) -> Result<T, String> {
    T::from_ssz(
        &read_snappy_ssz(&fork_choice_fixture_path(case, name)?, max_bytes)?,
        schema_preset(case)?,
    )
    .map_err(|error| error.to_string())
}

fn fields(value: &Value, allowed: &[&str]) -> Result<(), String> {
    let map = value.as_mapping().ok_or("operation fixture metadata must be a mapping")?;
    if map.keys().any(|key| key.as_str().is_none_or(|key| !allowed.contains(&key))) {
        return Err("operation fixture metadata contains unsupported fields".into());
    }
    Ok(())
}

fn compare(entry: &Value, result: Result<(), OperationGossipError>) -> Result<(), String> {
    let (outcome, reason) = match result {
        Ok(()) => ("valid", None),
        Err(OperationGossipError::Ignore(reason)) => ("ignore", Some(reason)),
        Err(OperationGossipError::Reject(reason)) => ("reject", Some(reason)),
        Err(error) => return Err(format!("operation gossip context/resource failure: {error}")),
    };
    let expected = entry["expected"].as_str().ok_or("operation expected outcome missing")?;
    if !matches!(expected, "valid" | "ignore" | "reject") || expected != outcome {
        return Err(format!("expected {expected}, received {outcome} ({reason:?})"));
    }
    if !entry["reason"].is_null() && entry["reason"].as_str() != reason {
        return Err(format!("expected reason {:?}, received {reason:?}", entry["reason"].as_str()));
    }
    Ok(())
}
