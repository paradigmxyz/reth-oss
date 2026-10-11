//! Authenticated fast-confirmation fixtures replay actual isolated BLS-disabled graph events.
//! All graph, FCR-variable and safe-execution-hash assertions are checked. Envelope fixtures
//! explicitly assume execution and DA results; no runtime proof is constructed here.

use super::{
    config_override, fixed_hex, fixture_gloas_parameters, fixture_transition_parameters,
    fork_choice_availability::FixtureAvailability, fork_choice_event_result,
    fork_choice_fixture_path, hex, poll_fixture_future, read_snappy_ssz, schema_fork,
    schema_preset, yaml, Case, FixtureExecutionEngine, Outcome,
};
use reth_embedded_beacon_fork_choice::{
    fast_confirmation::{FastConfirmation, FastConfirmationView, Parameters, Snapshot},
    gloas::{artificial::ArtificialSpecStore, DataAvailability, EnvelopeError},
    ForkChoiceNode, SpecStore, StoreError, StoreParameters,
};
use reth_embedded_beacon_primitives::{Fork, Root};
use reth_embedded_beacon_state_transition::ssz::{
    gloas, BeaconBlock, BeaconState, SignedBeaconBlock,
};
use serde_yaml::Value;

pub(super) fn supports(case: &Case) -> bool {
    case.runner == "fast_confirmation" &&
        matches!(
            case.fork.as_str(),
            "altair" | "bellatrix" | "capella" | "deneb" | "electra" | "fulu" | "gloas"
        )
}

pub(super) fn run_case(case: &Case, max_bytes: usize) -> Result<(), (Outcome, String)> {
    run(case, max_bytes).map_err(|error| (Outcome::Failed, error))
}

// This private type models the explicit DA assumption of the published Gloas fixture generator.
// Production confirmation receives only a graph whose actual import already required custody.
struct AssumedEnvelopeAvailability;
impl DataAvailability for AssumedEnvelopeAvailability {
    type Error = std::io::Error;
    async fn verify(&self, _root: Root, _block: &gloas::BeaconBlock) -> Result<(), Self::Error> {
        Ok(())
    }
}

enum Context {
    Classic { store: SpecStore, fcr: FastConfirmation<SpecStore> },
    Gloas { store: ArtificialSpecStore, fcr: FastConfirmation<ArtificialSpecStore> },
}
impl Context {
    fn tick(&mut self, time: u64) -> Result<(), StoreError> {
        match self {
            Self::Classic { store, .. } => store.on_tick(time),
            Self::Gloas { store, .. } => store.on_tick(time),
        }
    }
    fn block(
        &mut self,
        bytes: &[u8],
        case: &Case,
        step: &Value,
        engine: &FixtureExecutionEngine,
        max_bytes: usize,
    ) -> Result<(), String> {
        let preset = schema_preset(case)?;
        let fork = schema_fork(case)?;
        match self {
            Self::Classic { store, .. } => {
                let block = SignedBeaconBlock::from_ssz(bytes, fork, preset)
                    .map_err(|error| error.to_string())?;
                let availability = FixtureAvailability::new(
                    case,
                    step,
                    preset,
                    fixture_gloas_parameters(case, preset, max_bytes)?.fulu,
                    max_bytes,
                );
                let result = poll_fixture_future(store.on_block_and_contents_with_engine(
                    &block,
                    engine,
                    &availability,
                ))?;
                fork_choice_event_result(result, step)
            }
            Self::Gloas { store, .. } => {
                let block = gloas::SignedBeaconBlock::from_ssz(bytes, preset)
                    .map_err(|error| error.to_string())?;
                fork_choice_event_result(store.on_block_and_contents(&block), step)
            }
        }
    }
    fn attestation(&mut self, bytes: &[u8], case: &Case, step: &Value) -> Result<(), String> {
        let result = match self {
            Self::Classic { store, .. } => store.on_attestation_wire(bytes, schema_fork(case)?),
            Self::Gloas { store, .. } => store.on_attestation(
                &gloas::Attestation::from_ssz(bytes, schema_preset(case)?)
                    .map_err(|error| error.to_string())?,
            ),
        };
        fork_choice_event_result(result, step)
    }
    fn slashing(&mut self, bytes: &[u8], case: &Case, step: &Value) -> Result<(), String> {
        let result = match self {
            Self::Classic { store, .. } => {
                store.on_attester_slashing_wire(bytes, schema_fork(case)?)
            }
            Self::Gloas { store, .. } => store.on_attester_slashing(
                &gloas::AttesterSlashing::from_ssz(bytes, schema_preset(case)?)
                    .map_err(|error| error.to_string())?,
            ),
        };
        fork_choice_event_result(result, step)
    }
    fn envelope(
        &mut self,
        bytes: &[u8],
        case: &Case,
        step: &Value,
        engine: &FixtureExecutionEngine,
    ) -> Result<(), String> {
        let Self::Gloas { store, .. } = self else {
            return Err("execution envelope is only a Gloas fixture event".into())
        };
        let envelope = gloas::SignedExecutionPayloadEnvelope::from_ssz(bytes, schema_preset(case)?)
            .map_err(|error| error.to_string())?;
        let result = poll_fixture_future(store.on_execution_payload_envelope(
            &envelope,
            engine,
            &AssumedEnvelopeAvailability,
        ))?;
        let expected = match &step["valid"] {
            Value::Null => true,
            Value::Bool(value) => *value,
            _ => return Err("envelope validity must be a boolean".into()),
        };
        match result {
            Err(
                error @ EnvelopeError::Store(
                    StoreError::ResourceLimit |
                    StoreError::ValidationUnavailable(_) |
                    StoreError::Overflow,
                ),
            ) => Err(error.to_string()),
            Err(error) if expected => Err(error.to_string()),
            Ok(()) if !expected => Err("expected envelope rejection, received success".into()),
            Err(_) | Ok(()) => Ok(()),
        }
    }
    fn check(&mut self, checks: &Value) -> Result<(), String> {
        let update = !checks["confirmed_root"].is_null();
        match self {
            Self::Classic { store, fcr } => {
                if update {
                    fcr.on_fast_confirmation(store).map_err(|error| error.to_string())?;
                }
                check(store, fcr, store.proposer_boost_root(), checks)
            }
            Self::Gloas { store, fcr } => {
                if update {
                    fcr.on_fast_confirmation(store).map_err(|error| error.to_string())?;
                }
                check(store, fcr, store.proposer_boost_root(), checks)
            }
        }
    }
}

fn run(case: &Case, max_bytes: usize) -> Result<(), String> {
    let meta = yaml(&case.path.join("meta.yaml"), max_bytes)?;
    if meta["bls_setting"].as_u64() != Some(2) {
        return Err(
            "fast-confirmation artificial fixture requires explicit BLS-disabled metadata".into()
        )
    }
    let preset = schema_preset(case)?;
    let fork = schema_fork(case)?;
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
        .ok_or("FCR slot duration overflow")?;
    parameters.proposer_score_boost =
        config_override(case, "PROPOSER_SCORE_BOOST", parameters.proposer_score_boost, max_bytes)?;
    parameters.attestation_due_bps =
        config_override(case, "ATTESTATION_DUE_BPS", parameters.attestation_due_bps, max_bytes)?;
    parameters.gloas_attestation_due_bps = config_override(
        case,
        "ATTESTATION_DUE_BPS_GLOAS",
        parameters.gloas_attestation_due_bps,
        max_bytes,
    )?;
    let fcr_parameters = Parameters {
        byzantine_threshold: config_override(
            case,
            "CONFIRMATION_BYZANTINE_THRESHOLD",
            25,
            max_bytes,
        )?,
        ..Parameters::default()
    };
    let state = read_snappy_ssz(&case.path.join("anchor_state.ssz_snappy"), max_bytes)?;
    let block = read_snappy_ssz(&case.path.join("anchor_block.ssz_snappy"), max_bytes)?;
    let mut context = if fork == Fork::Gloas {
        let state =
            gloas::BeaconState::from_ssz(&state, preset).map_err(|error| error.to_string())?;
        let block =
            gloas::BeaconBlock::from_ssz(&block, preset).map_err(|error| error.to_string())?;
        let store = ArtificialSpecStore::get_store(state, block, parameters, processing)
            .map_err(|error| error.to_string())?;
        let fcr =
            FastConfirmation::new(&store, fcr_parameters).map_err(|error| error.to_string())?;
        Context::Gloas { store, fcr }
    } else {
        let state =
            BeaconState::from_ssz(&state, fork, preset).map_err(|error| error.to_string())?;
        let block =
            BeaconBlock::from_ssz(&block, fork, preset).map_err(|error| error.to_string())?;
        let mut store =
            SpecStore::get_store(state, block, parameters).map_err(|error| error.to_string())?;
        store
            .set_transition_parameters(fixture_transition_parameters(
                case, preset, fork, 0, max_bytes,
            )?)
            .map_err(|error| error.to_string())?;
        let fcr =
            FastConfirmation::new(&store, fcr_parameters).map_err(|error| error.to_string())?;
        Context::Classic { store, fcr }
    };
    let metadata = yaml(&case.path.join("steps.yaml"), max_bytes)?;
    let steps = metadata.as_sequence().ok_or("fast-confirmation steps must be a sequence")?;
    if steps.is_empty() || steps.len() > 100_000 {
        return Err("fast-confirmation event budget exceeded".into())
    }
    let engine = FixtureExecutionEngine::from_case(case, max_bytes, false)?;
    let mut assertions = 0;
    for (index, step) in steps.iter().enumerate() {
        let mapping = step.as_mapping().ok_or("fast-confirmation event must be a mapping")?;
        if mapping.keys().any(|key| {
            !matches!(
                key.as_str(),
                Some(
                    "tick" |
                        "checks" |
                        "block" |
                        "attestation" |
                        "attester_slashing" |
                        "execution_payload" |
                        "valid" |
                        "blobs" |
                        "proofs" |
                        "columns"
                )
            )
        }) {
            return Err(format!("unknown fast-confirmation event key at step {index}"))
        }
        let events =
            ["tick", "checks", "block", "attestation", "attester_slashing", "execution_payload"];
        let selected = events.into_iter().filter(|key| !step[*key].is_null()).collect::<Vec<_>>();
        if selected.len() != 1 {
            return Err(format!("fast-confirmation step {index} must contain exactly one event"))
        }
        let event = selected[0];
        let result = if event == "tick" {
            fork_choice_event_result(
                context.tick(
                    step[event]
                        .as_u64()
                        .and_then(|time| time.checked_mul(1000))
                        .ok_or("FCR tick overflow")?,
                ),
                step,
            )
        } else if event == "checks" {
            assertions += 1;
            context.check(&step[event])
        } else {
            let bytes = read_snappy_ssz(&fork_choice_fixture_path(case, &step[event])?, max_bytes)?;
            match event {
                "block" => context.block(&bytes, case, step, &engine, max_bytes),
                "attestation" => context.attestation(&bytes, case, step),
                "attester_slashing" => context.slashing(&bytes, case, step),
                "execution_payload" => context.envelope(&bytes, case, step, &engine),
                _ => unreachable!("selected events enumerated above"),
            }
        };
        result.map_err(|error| format!("step {index}: {error}"))?;
    }
    if assertions == 0 {
        return Err("fast-confirmation fixture contains no actual assertions".into())
    }
    Ok(())
}

fn check<G: FastConfirmationView>(
    graph: &G,
    fcr: &FastConfirmation<G>,
    boost: Root,
    checks: &Value,
) -> Result<(), String> {
    let map = checks.as_mapping().ok_or("FCR assertions must be a mapping")?;
    if map.is_empty() {
        return Err("FCR assertion mapping is empty".into())
    }
    if map.keys().any(|key| {
        !matches!(
            key.as_str(),
            Some(
                "time" |
                    "head" |
                    "justified_checkpoint" |
                    "finalized_checkpoint" |
                    "proposer_boost_root" |
                    "previous_epoch_observed_justified_checkpoint" |
                    "current_epoch_observed_justified_checkpoint" |
                    "previous_epoch_greatest_unrealized_checkpoint" |
                    "previous_slot_head" |
                    "current_slot_head" |
                    "confirmed_root" |
                    "safe_execution_block_hash"
            )
        )
    }) {
        return Err("unknown required FCR assertion".into())
    }
    if checks["time"].as_u64().and_then(|time| time.checked_mul(1000)) != Some(graph.time_ms()) {
        return Err("FCR graph time mismatch".into())
    }
    let head = graph.head().map_err(|error| error.to_string())?;
    assert_head(graph, head, &checks["head"])?;
    let snapshot = fcr.snapshot();
    for (name, checkpoint) in [
        ("justified_checkpoint", graph.checkpoints().0),
        ("finalized_checkpoint", graph.checkpoints().1),
        (
            "previous_epoch_observed_justified_checkpoint",
            snapshot.previous_epoch_observed_justified_checkpoint,
        ),
        (
            "current_epoch_observed_justified_checkpoint",
            snapshot.current_epoch_observed_justified_checkpoint,
        ),
        (
            "previous_epoch_greatest_unrealized_checkpoint",
            snapshot.previous_epoch_greatest_unrealized_checkpoint,
        ),
    ] {
        if !checks[name].is_null() &&
            (checks[name]["epoch"].as_u64() != Some(checkpoint.epoch) ||
                fixed_hex::<32>(&checks[name]["root"])? != checkpoint.root)
        {
            return Err(format!("FCR {name} mismatch: actual {checkpoint:?}"))
        }
    }
    assert_roots(snapshot, boost, checks)?;
    if !checks["safe_execution_block_hash"].is_null() {
        let actual = fcr.safe_execution_block_hash(graph).map_err(|error| error.to_string())?;
        if fixed_hex::<32>(&checks["safe_execution_block_hash"])? != actual {
            return Err(format!("FCR safe execution hash differs, actual {}", hex(&actual)))
        }
    }
    Ok(())
}
fn assert_head<G: FastConfirmationView>(
    graph: &G,
    head: ForkChoiceNode,
    expected: &Value,
) -> Result<(), String> {
    let actual = graph.block_info(head.root).map_err(|error| error.to_string())?;
    if expected["root"].is_null() ||
        fixed_hex::<32>(&expected["root"])? != head.root ||
        expected["slot"].as_u64() != Some(actual.slot)
    {
        return Err(format!(
            "FCR head mismatch: actual root {}, slot {}",
            hex(&head.root),
            actual.slot
        ))
    }
    if !expected["payload_status"].is_null() &&
        expected["payload_status"].as_u64() != head.payload_status.map(|status| status as u64)
    {
        return Err("FCR head payload branch mismatch".into())
    }
    Ok(())
}
fn assert_roots(snapshot: Snapshot, boost: Root, checks: &Value) -> Result<(), String> {
    for (name, actual) in [
        ("proposer_boost_root", boost),
        ("previous_slot_head", snapshot.previous_slot_head),
        ("current_slot_head", snapshot.current_slot_head),
        ("confirmed_root", snapshot.confirmed_root),
    ] {
        if !checks[name].is_null() && fixed_hex::<32>(&checks[name])? != actual {
            return Err(format!("FCR {name} differs: actual {}", hex(&actual)))
        }
    }
    Ok(())
}
