//! Published optimistic-sync events with explicit synthetic execution status callbacks.
//! All consensus signatures/transitions/post-state roots remain production checks; the
//! engine responses in this adapter are primary fixture assumptions, not live EL evidence.

use super::{
    config_override, fixed_hex, fixture_transition_parameters, fork_choice_availability,
    fork_choice_fixture_path, hex, poll_fixture_future, read_snappy_ssz, schema_fork,
    schema_preset, yaml, Case, Outcome,
};
use reth_embedded_beacon_fork_choice::{
    store::optimistic::{
        ExecutionEngine, ImportOutcome, OptimisticParameters, OptimisticStore, Payload,
        PayloadContext, PayloadResponse, PayloadStatus, TerminalResponse,
    },
    StoreParameters,
};
use reth_embedded_beacon_primitives::{Checkpoint, Root};
use reth_embedded_beacon_state_transition::ssz::{BeaconBlock, BeaconState, SignedBeaconBlock};
use std::{
    collections::BTreeMap,
    io,
    sync::{Arc, Mutex},
};

pub(super) fn supports(case: &Case) -> bool {
    case.runner == "sync" &&
        case.handler == "optimistic" &&
        matches!(case.fork.as_str(), "bellatrix" | "capella" | "deneb" | "electra" | "fulu")
}

pub(super) fn run_case(case: &Case, max_bytes: usize) -> Result<(), (Outcome, String)> {
    run(case, max_bytes).map_err(|error| (Outcome::Failed, error))
}

fn run(case: &Case, max_bytes: usize) -> Result<(), String> {
    let preset = schema_preset(case)?;
    let fork = schema_fork(case)?;
    let transitions = fixture_transition_parameters(case, preset, fork, 0, max_bytes)?;
    let state = BeaconState::from_ssz(
        &read_snappy_ssz(&case.path.join("anchor_state.ssz_snappy"), max_bytes)?,
        fork,
        preset,
    )
    .map_err(|error| error.to_string())?;
    let block = BeaconBlock::from_ssz(
        &read_snappy_ssz(&case.path.join("anchor_block.ssz_snappy"), max_bytes)?,
        fork,
        preset,
    )
    .map_err(|error| error.to_string())?;
    let phase0 = transitions.gloas.fulu.electra.deneb.capella.bellatrix.altair.phase0;
    let altair = transitions.gloas.fulu.electra.deneb.capella.bellatrix.altair;
    let mut parameters = StoreParameters::for_preset(preset);
    parameters.phase0 = phase0;
    parameters.altair_fork_version = altair.fork_version;
    parameters.inactivity_score_bias = altair.inactivity_score_bias;
    parameters.inactivity_score_recovery_rate = altair.inactivity_score_recovery_rate;
    parameters.slot_duration_ms = transitions
        .gloas
        .fulu
        .electra
        .deneb
        .capella
        .bellatrix
        .seconds_per_slot
        .checked_mul(1000)
        .ok_or("slot clock overflow")?;
    let optimism = OptimisticParameters {
        safe_slots_to_import_optimistically: config_override(
            case,
            "SAFE_SLOTS_TO_IMPORT_OPTIMISTICALLY",
            128,
            max_bytes,
        )?,
        ..OptimisticParameters::default()
    };
    let availability_parameters = transitions.gloas.fulu.clone();
    let engine = Arc::new(FixtureEngine::default());
    let mut store =
        OptimisticStore::get_store(state, block, parameters, transitions, optimism, engine.clone())
            .map_err(|error| error.to_string())?;
    let steps = yaml(&case.path.join("steps.yaml"), max_bytes)?;
    let steps = steps.as_sequence().ok_or("optimistic fixture steps must be a sequence")?;
    if steps.is_empty() || steps.len() > 16384 {
        return Err("optimistic step budget exceeded or empty".into())
    }
    let mut assertions = 0;
    for (index, step) in steps.iter().enumerate() {
        let event_count =
            ["tick", "checks", "block", "block_hash", "attestation", "attester_slashing"]
                .iter()
                .filter(|name| !step[**name].is_null())
                .count();
        if event_count != 1 {
            return Err(format!("optimistic step {index} must contain exactly one event"))
        }
        if !step["tick"].is_null() {
            let time = step["tick"]
                .as_u64()
                .and_then(|value| value.checked_mul(1000))
                .ok_or("invalid optimistic clock")?;
            store
                .on_tick(time)
                .map_err(|error| format!("optimistic tick step {index}: {error}"))?;
        } else if !step["block_hash"].is_null() {
            let hash = fixed_hex::<32>(&step["block_hash"])?;
            let response = &step["payload_status"];
            let status = match response["status"].as_str() {
                Some("VALID") => PayloadStatus::Valid,
                Some("SYNCING") => PayloadStatus::Syncing,
                Some("ACCEPTED") => PayloadStatus::Accepted,
                Some("INVALID") => PayloadStatus::Invalid,
                Some("INVALID_BLOCK_HASH") => PayloadStatus::InvalidBlockHash,
                _ => return Err("unknown optimistic engine status".into()),
            };
            let latest_valid_hash = if response["latest_valid_hash"].is_null() {
                None
            } else {
                Some(fixed_hex::<32>(&response["latest_valid_hash"])?)
            };
            let validation_error = if response["validation_error"].is_null() {
                None
            } else {
                Some(
                    response["validation_error"]
                        .as_str()
                        .ok_or("engine validation error must be text")?
                        .to_owned(),
                )
            };
            engine.insert(hash, PayloadResponse { status, latest_valid_hash, validation_error })?;
        } else if !step["block"].is_null() {
            let block = SignedBeaconBlock::from_ssz(
                &read_snappy_ssz(&fork_choice_fixture_path(case, &step["block"])?, max_bytes)?,
                fork,
                preset,
            )
            .map_err(|error| error.to_string())?;
            let availability = fork_choice_availability::FixtureAvailability::new(
                case,
                step,
                preset,
                availability_parameters.clone(),
                max_bytes,
            );
            let actual = poll_fixture_future(store.on_block(&block, &availability))?
                .map_err(|error| format!("optimistic block step {index}: {error}"))?;
            let expected = step["valid"].as_bool().unwrap_or(true);
            if matches!(actual, ImportOutcome::Accepted { .. }) != expected {
                return Err(format!(
                    "optimistic block step {index}: expected valid={expected}, received {actual:?}"
                ))
            }
        } else if !step["attestation"].is_null() {
            let bytes =
                read_snappy_ssz(&fork_choice_fixture_path(case, &step["attestation"])?, max_bytes)?;
            store
                .on_attestation_wire(&bytes, fork)
                .map_err(|error| format!("optimistic vote step {index}: {error}"))?;
        } else if !step["attester_slashing"].is_null() {
            let bytes = read_snappy_ssz(
                &fork_choice_fixture_path(case, &step["attester_slashing"])?,
                max_bytes,
            )?;
            store
                .on_attester_slashing_wire(&bytes, fork)
                .map_err(|error| format!("optimistic slashing step {index}: {error}"))?;
        } else {
            let checks = &step["checks"];
            let mapping = checks.as_mapping().ok_or("optimistic checks must be a mapping")?;
            if mapping.is_empty() {
                return Err("empty optimistic assertions cannot pass".into())
            }
            for key in mapping.keys() {
                if !matches!(
                    key.as_str(),
                    Some(
                        "head" |
                            "time" |
                            "justified_checkpoint" |
                            "finalized_checkpoint" |
                            "proposer_boost_root"
                    )
                ) {
                    return Err("unsupported optimistic assertion key".into())
                }
            }
            assertions += 1;
            if !checks["head"].is_null() {
                let actual = store.head().map_err(|error| error.to_string())?;
                if (!checks["head"]["root"].is_null() &&
                    fixed_hex::<32>(&checks["head"]["root"])? != actual.root) ||
                    (!checks["head"]["slot"].is_null() &&
                        checks["head"]["slot"].as_u64() != Some(actual.slot))
                {
                    return Err(format!(
                        "optimistic head mismatch step {index}: root {}, slot {}, optimistic {}",
                        hex(&actual.root),
                        actual.slot,
                        actual.execution_optimistic
                    ))
                }
            }
            if !checks["time"].is_null() &&
                checks["time"].as_u64().and_then(|value| value.checked_mul(1000)) !=
                    Some(store.time_ms())
            {
                return Err(format!("optimistic clock mismatch step {index}"))
            }
            let (justified, finalized) = store.checkpoints().map_err(|error| error.to_string())?;
            for (name, actual) in
                [("justified_checkpoint", justified), ("finalized_checkpoint", finalized)]
            {
                if !checks[name].is_null() {
                    let expected = Checkpoint {
                        epoch: checks[name]["epoch"]
                            .as_u64()
                            .ok_or("checkpoint epoch must be unsigned")?,
                        root: fixed_hex::<32>(&checks[name]["root"])?,
                    };
                    if expected != actual {
                        return Err(format!("optimistic {name} mismatch step {index}"))
                    }
                }
            }
            if !checks["proposer_boost_root"].is_null() &&
                fixed_hex::<32>(&checks["proposer_boost_root"])? !=
                    store.proposer_boost_root().map_err(|error| error.to_string())?
            {
                return Err(format!("optimistic proposer boost mismatch step {index}"))
            }
        }
    }
    if assertions == 0 {
        return Err("optimistic fixture contains no checked output".into())
    }
    Ok(())
}

#[derive(Default)]
struct FixtureEngine {
    responses: Mutex<BTreeMap<Root, PayloadResponse>>,
}
impl FixtureEngine {
    fn insert(&self, hash: Root, response: PayloadResponse) -> Result<(), String> {
        let mut records = self.responses.lock().map_err(|_| "fixture response cache poisoned")?;
        if records.len() >= 4096 && !records.contains_key(&hash) {
            return Err("fixture response cache exceeds bound".into())
        }
        records.insert(hash, response);
        Ok(())
    }
}
impl ExecutionEngine for FixtureEngine {
    type Error = io::Error;
    async fn verify_terminal_block(
        &self,
        _: Payload<'_>,
        _: PayloadContext,
    ) -> Result<TerminalResponse, Self::Error> {
        // This synthetic EL fixture assumes terminal eligibility; it is not live ancestry proof.
        Ok(TerminalResponse::Verified)
    }
    async fn notify_new_payload(
        &self,
        payload: Payload<'_>,
        _: PayloadContext,
    ) -> Result<PayloadResponse, Self::Error> {
        self.responses
            .lock()
            .map_err(|_| io::Error::other("fixture status cache poisoned"))?
            .get(&payload.block_hash())
            .cloned()
            .ok_or_else(|| io::Error::other("missing declared exact-payload fixture response"))
    }
}
