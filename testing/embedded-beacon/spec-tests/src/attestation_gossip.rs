//! Published attestation gossip fixtures with strict signed setup and exact propagation outcomes.
//!
//! Declared finality, payload status and failed setup blocks are isolated observations. Every
//! candidate signature remains mandatory; no fixture result is used as validation authority.

use super::{
    block_gossip::{load_fixture_store, FixtureStore},
    fork_choice_fixture_path, read_snappy_ssz, schema_fork_name, schema_preset, yaml, Case,
    Outcome,
};
use reth_embedded_beacon_node::attestation_gossip::{
    AttestationGossipError, LegacyAttestation, SeenAttestations, SignedAggregate, SingleAttestation,
};
use reth_embedded_beacon_primitives::Fork;
use reth_embedded_beacon_state_transition::ssz::{
    altair, auxiliary, bellatrix, capella, deneb, phase0, Preset,
};
use serde_yaml::Value;

pub(super) fn supports(case: &Case) -> bool {
    case.runner == "networking" &&
        matches!(
            case.handler.as_str(),
            "gossip_beacon_attestation" | "gossip_beacon_aggregate_and_proof"
        ) &&
        super::FORKS.contains(&case.fork.as_str()) &&
        matches!(case.preset.as_str(), "mainnet" | "minimal")
}

pub(super) fn uses_execution_mock(case: &Case, max_bytes: usize) -> bool {
    supports(case) &&
        matches!(case.fork.as_str(), "bellatrix" | "capella" | "deneb" | "electra" | "fulu") &&
        yaml(&case.path.join("meta.yaml"), max_bytes).is_ok_and(|meta| {
            meta["blocks"].as_sequence().is_some_and(|blocks| {
                blocks.iter().skip(1).any(|block| block["failed"].as_bool() != Some(true))
            })
        })
}

pub(super) fn uses_asserted_graph(case: &Case, max_bytes: usize) -> bool {
    supports(case) &&
        yaml(&case.path.join("meta.yaml"), max_bytes).is_ok_and(|meta| {
            !meta["finalized_checkpoint"].is_null() ||
                meta["blocks"].as_sequence().is_some_and(|blocks| {
                    blocks.iter().any(|block| {
                        block["failed"].as_bool() == Some(true) ||
                            !block["payload_status"].is_null() ||
                            !block["payload"].is_null()
                    })
                })
        })
}

pub(super) fn run_case(case: &Case, max_bytes: usize) -> Result<(), (Outcome, String)> {
    run(case, max_bytes).map_err(|error| (Outcome::Failed, error))
}

fn run(case: &Case, max_bytes: usize) -> Result<(), String> {
    let preset = schema_preset(case)?;
    let fork = schema_fork_name(&case.fork)?;
    let meta = yaml(&case.path.join("meta.yaml"), max_bytes)?;
    fields(&meta, &["topic", "blocks", "messages", "finalized_checkpoint", "bls_setting"])?;
    if !meta["bls_setting"].is_null() && meta["bls_setting"].as_u64() != Some(1) {
        return Err("attestation gossip requires real fixture signatures".into())
    }
    let topic = case.handler.strip_prefix("gossip_").ok_or("missing attestation gossip topic")?;
    if meta["topic"].as_str() != Some(topic) {
        return Err("attestation gossip topic differs from handler".into())
    }
    let store = load_fixture_store(case, &meta, max_bytes)?;
    let messages =
        meta["messages"].as_sequence().ok_or("attestation gossip messages must be a sequence")?;
    if messages.is_empty() || messages.len() > 4096 {
        return Err("attestation gossip message budget exceeded".into())
    }
    let mut seen =
        SeenAttestations::new(262_144, 16 * 1024 * 1024).map_err(|error| error.to_string())?;
    for (index, entry) in messages.iter().enumerate() {
        let allowed = if topic == "beacon_attestation" {
            &["message", "current_time_ms", "subnet_id", "expected", "reason"][..]
        } else {
            &["message", "current_time_ms", "expected", "reason"][..]
        };
        fields(entry, allowed)?;
        let time = unsigned(&entry["current_time_ms"])?;
        let bytes =
            read_snappy_ssz(&fork_choice_fixture_path(case, &entry["message"])?, max_bytes)?;
        let result = match topic {
            "beacon_attestation" => {
                let subnet = unsigned(&entry["subnet_id"])?;
                validate_attestation(&store, &mut seen, fork, preset, &bytes, time, subnet)?
            }
            "beacon_aggregate_and_proof" => {
                validate_aggregate(&store, &mut seen, fork, preset, &bytes, time)?
            }
            _ => return Err("unsupported attestation gossip topic".into()),
        };
        compare(entry, result)
            .map_err(|error| format!("attestation gossip event {index}: {error}"))?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn validate_attestation(
    store: &FixtureStore,
    seen: &mut SeenAttestations,
    fork: Fork,
    preset: Preset,
    bytes: &[u8],
    time: u64,
    subnet: u64,
) -> Result<Result<(), AttestationGossipError>, String> {
    macro_rules! legacy {
        ($module:ident, $variant:ident, $store:expr) => {{
            let operation =
                $module::Attestation::from_ssz(bytes, preset).map_err(|error| error.to_string())?;
            seen.validate_attestation(
                $store.as_ref(),
                LegacyAttestation::$variant(&operation),
                time,
                subnet,
            )
        }};
    }
    macro_rules! single {
        ($module:ident, $variant:ident, $store:expr) => {{
            let operation = auxiliary::$module::SingleAttestation::from_ssz(bytes, preset)
                .map_err(|error| error.to_string())?;
            seen.validate_single(
                $store.as_ref(),
                SingleAttestation::$variant(&operation),
                time,
                subnet,
            )
        }};
    }
    Ok(match (store, fork) {
        (FixtureStore::Classic(store), Fork::Phase0) => legacy!(phase0, Phase0, store),
        (FixtureStore::Classic(store), Fork::Altair) => legacy!(altair, Altair, store),
        (FixtureStore::Classic(store), Fork::Bellatrix) => legacy!(bellatrix, Bellatrix, store),
        (FixtureStore::Classic(store), Fork::Capella) => legacy!(capella, Capella, store),
        (FixtureStore::Classic(store), Fork::Deneb) => legacy!(deneb, Deneb, store),
        (FixtureStore::Classic(store), Fork::Electra) => single!(electra, Electra, store),
        (FixtureStore::Classic(store), Fork::Fulu) => single!(fulu, Fulu, store),
        (FixtureStore::Gloas(store), Fork::Gloas) => {
            let operation = auxiliary::gloas::SingleAttestation::from_ssz(bytes, preset)
                .map_err(|error| error.to_string())?;
            seen.validate_gloas_single(store.as_ref(), &operation, time, subnet)
        }
        _ => return Err("attestation gossip store and wire fork differ".into()),
    })
}

fn validate_aggregate(
    store: &FixtureStore,
    seen: &mut SeenAttestations,
    fork: Fork,
    preset: Preset,
    bytes: &[u8],
    time: u64,
) -> Result<Result<(), AttestationGossipError>, String> {
    macro_rules! aggregate {
        ($module:ident, $variant:ident, $store:expr) => {{
            let operation = auxiliary::$module::SignedAggregateAndProof::from_ssz(bytes, preset)
                .map_err(|error| error.to_string())?;
            seen.validate_aggregate($store.as_ref(), SignedAggregate::$variant(&operation), time)
        }};
    }
    Ok(match (store, fork) {
        (FixtureStore::Classic(store), Fork::Phase0) => aggregate!(phase0, Phase0, store),
        (FixtureStore::Classic(store), Fork::Altair) => aggregate!(altair, Altair, store),
        (FixtureStore::Classic(store), Fork::Bellatrix) => aggregate!(bellatrix, Bellatrix, store),
        (FixtureStore::Classic(store), Fork::Capella) => aggregate!(capella, Capella, store),
        (FixtureStore::Classic(store), Fork::Deneb) => aggregate!(deneb, Deneb, store),
        (FixtureStore::Classic(store), Fork::Electra) => aggregate!(electra, Electra, store),
        (FixtureStore::Classic(store), Fork::Fulu) => aggregate!(fulu, Fulu, store),
        (FixtureStore::Gloas(store), Fork::Gloas) => {
            let operation = auxiliary::gloas::SignedAggregateAndProof::from_ssz(bytes, preset)
                .map_err(|error| error.to_string())?;
            seen.validate_gloas_aggregate(store.as_ref(), &operation, time)
        }
        _ => return Err("aggregate gossip store and wire fork differ".into()),
    })
}

fn fields(value: &Value, allowed: &[&str]) -> Result<(), String> {
    let mapping = value.as_mapping().ok_or("attestation gossip metadata must be a mapping")?;
    if mapping.keys().any(|key| !key.as_str().is_some_and(|key| allowed.contains(&key))) {
        return Err("unknown attestation gossip metadata field".into())
    }
    Ok(())
}

fn unsigned(value: &Value) -> Result<u64, String> {
    value.as_u64().ok_or_else(|| "attestation gossip scalar must be unsigned".into())
}

fn compare(entry: &Value, result: Result<(), AttestationGossipError>) -> Result<(), String> {
    let (actual, reason) = match result {
        Ok(()) => ("valid", None),
        Err(AttestationGossipError::Ignore(reason)) => ("ignore", Some(reason)),
        Err(AttestationGossipError::Reject(reason)) => ("reject", Some(reason)),
        Err(error) => return Err(format!("attestation gossip infrastructure failure: {error}")),
    };
    let expected =
        entry["expected"].as_str().ok_or("attestation gossip expected result must be a string")?;
    let expected_reason = if entry["reason"].is_null() {
        None
    } else {
        Some(entry["reason"].as_str().ok_or("attestation gossip reason must be a string")?)
    };
    if actual != expected || reason != expected_reason {
        return Err(format!("expected {expected}/{expected_reason:?}, received {actual}/{reason:?}"))
    }
    Ok(())
}
