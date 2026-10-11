//! Authenticated full-node light-client collection cases with strict consensus replay.
//! Later execution payload imports use the separately reported fixture-only EL adapter.

use super::{
    light_client::{check_digest, digest_schema, named_bytes, parameters},
    poll_fixture_future, schema_fork, schema_preset, signed_fixture_slot, yaml, Case,
    FixtureExecutionEngine, Outcome,
};
use reth_embedded_beacon_primitives::{root_from_hex, Root};
use reth_embedded_beacon_state_transition::{
    fork_processing,
    light_client::{
        data_collection::{LightClientDataLimits, LightClientDataStore},
        full_node, LightClientParameters,
    },
    ssz::{self, BeaconState, SignedBeaconBlock, Ssz},
};
use serde_yaml::Value;
use std::collections::BTreeMap;

pub(super) fn supports(case: &Case) -> bool {
    case.runner == "light_client" && case.handler == "data_collection"
}
pub(super) fn run_case(case: &Case, max_bytes: usize) -> Result<(), (Outcome, String)> {
    run(case, max_bytes).map_err(|error| (Outcome::Failed, error))
}
fn root(value: &Value) -> Result<Root, String> {
    root_from_hex(value.as_str().ok_or("missing root string")?).map_err(|error| error.to_string())
}
fn initial_block(state: &BeaconState, preset: ssz::Preset) -> Result<SignedBeaconBlock, String> {
    let state_root = state.hash_tree_root().map_err(|error| error.to_string())?;
    macro_rules! block {
        ($fork:ident, $module:ident) => {{
            let mut value = <ssz::$module::SignedBeaconBlock as Ssz>::default_value(preset)
                .map_err(|error| error.to_string())?;
            value.message.state_root = state_root;
            SignedBeaconBlock::$fork(value, preset)
        }};
    }
    // The fixture generator uses the declared default body; Gloas binds the genesis bid.
    Ok(match state {
        BeaconState::Phase0(_, _) => block!(Phase0, phase0),
        BeaconState::Altair(_, _) => block!(Altair, altair),
        BeaconState::Bellatrix(_, _) => block!(Bellatrix, bellatrix),
        BeaconState::Capella(_, _) => block!(Capella, capella),
        BeaconState::Deneb(_, _) => block!(Deneb, deneb),
        BeaconState::Electra(_, _) => block!(Electra, electra),
        BeaconState::Fulu(_, _) => block!(Fulu, fulu),
        BeaconState::Gloas(data, _) => {
            let mut value = <ssz::gloas::SignedBeaconBlock as Ssz>::default_value(preset)
                .map_err(|error| error.to_string())?;
            value.message.state_root = state_root;
            value.message.body.signed_execution_payload_bid.message =
                data.latest_execution_payload_bid.clone();
            SignedBeaconBlock::Gloas(value, preset)
        }
    })
}
fn compare_bytes(
    case: &Case,
    expected: &Value,
    actual: Option<Vec<u8>>,
    slot: Option<u64>,
    params: &LightClientParameters,
    max_bytes: usize,
) -> Result<(), String> {
    if expected.is_null() {
        return if actual.is_none() {
            Ok(())
        } else {
            Err("unexpected served light-client object".into())
        };
    }
    let actual = actual.ok_or("required served light-client object is absent")?;
    let fork = digest_schema(&expected["fork_digest"], params)?;
    check_digest(
        &expected["fork_digest"],
        params,
        fork,
        slot.ok_or("missing served header slot")?,
    )?;
    let name = expected["data"].as_str().ok_or("missing served fixture member")?;
    if actual != named_bytes(case, name, max_bytes)? {
        return Err(format!("served object differs from complete published bytes: {name}"));
    }
    Ok(())
}
fn checks(
    case: &Case,
    checks: &Value,
    store: &LightClientDataStore,
    params: &LightClientParameters,
    max_bytes: usize,
) -> Result<(), String> {
    let checkpoint = store.latest_finalized_checkpoint();
    if checks["latest_finalized_checkpoint"]["epoch"].as_u64() != Some(checkpoint.epoch) ||
        root(&checks["latest_finalized_checkpoint"]["root"])? != checkpoint.root
    {
        return Err("served finalized checkpoint differs from published expectation".into());
    }
    let preset = params.preset();
    for entry in checks["bootstraps"].as_sequence().ok_or("missing bootstrap checks")? {
        let actual = store.bootstrap(&root(&entry["block_root"])?);
        compare_bytes(
            case,
            &entry["bootstrap"],
            actual
                .map(|value| value.to_ssz(preset))
                .transpose()
                .map_err(|error| error.to_string())?,
            actual.map(|value| value.header.beacon().slot),
            params,
            max_bytes,
        )?;
    }
    for entry in checks["best_updates"].as_sequence().ok_or("missing best-update checks")? {
        let actual = store.best_update(entry["period"].as_u64().ok_or("missing update period")?);
        compare_bytes(
            case,
            &entry["update"],
            actual
                .map(|value| value.to_ssz(preset))
                .transpose()
                .map_err(|error| error.to_string())?,
            actual.map(|value| value.attested_header.beacon().slot),
            params,
            max_bytes,
        )?;
    }
    let finality = store.latest_finality_update();
    compare_bytes(
        case,
        &checks["latest_finality_update"],
        finality
            .map(|value| value.to_ssz(preset))
            .transpose()
            .map_err(|error| error.to_string())?,
        finality.map(|value| value.attested_header.beacon().slot),
        params,
        max_bytes,
    )?;
    let optimistic = store.latest_optimistic_update();
    compare_bytes(
        case,
        &checks["latest_optimistic_update"],
        optimistic
            .map(|value| value.to_ssz(preset))
            .transpose()
            .map_err(|error| error.to_string())?,
        optimistic.map(|value| value.attested_header.beacon().slot),
        params,
        max_bytes,
    )
}
fn run(case: &Case, max_bytes: usize) -> Result<(), String> {
    let preset = schema_preset(case)?;
    let bytes = named_bytes(case, "initial_state", max_bytes)?;
    let initial = BeaconState::from_ssz(&bytes, schema_fork(case)?, preset)
        .map_err(|error| error.to_string())?;
    let params = parameters(case, preset, initial.genesis_validators_root(), max_bytes)?;
    let block = initial_block(&initial, preset)?;
    let anchor = block.message_root().map_err(|error| error.to_string())?;
    let mut store = LightClientDataStore::new(
        &initial,
        &block,
        &params,
        LightClientDataLimits { max_blocks: 4096, max_encoded_bytes: 64 * 1024 * 1024 },
    )
    .map_err(|error| error.to_string())?;
    // Fixture-only history enables genuine replay and exact fork links; the production cache
    // retains neither complete states nor payloads. Its separate hard budgets are enforced.
    let mut history = BTreeMap::from([(anchor, (initial, block))]);
    let engine = FixtureExecutionEngine::from_case(case, max_bytes, false)?;
    let steps = yaml(&case.path.join("steps.yaml"), max_bytes)?;
    let steps = steps.as_sequence().ok_or("collection steps must be a sequence")?;
    if steps.len() > 100_000 {
        return Err("collection steps exceed bound".into());
    }
    for (index, step) in steps.iter().enumerate() {
        let result = (|| -> Result<(), String> {
            let mapping = step.as_mapping().ok_or("collection step must be mapping")?;
            if mapping.len() != 1 {
                return Err("collection step must contain one action".into());
            }
            let (kind, action) = mapping.iter().next().ok_or("empty collection action")?;
            match kind.as_str() {
                Some("new_block") => {
                    let bytes = named_bytes(
                        case,
                        action["data"].as_str().ok_or("missing block member")?,
                        max_bytes,
                    )?;
                    let slot = signed_fixture_slot(&bytes)?;
                    let fork =
                        params.transitions.fork_at_slot(slot).map_err(|error| error.to_string())?;
                    check_digest(&action["fork_digest"], &params, fork, slot)?;
                    let block = SignedBeaconBlock::from_ssz(&bytes, fork, preset)
                        .map_err(|error| error.to_string())?;
                    let header = block.header().map_err(|error| error.to_string())?;
                    let (parent_state, parent_block) = history
                        .get(&header.parent_root)
                        .ok_or("fixture parent state unavailable")?;
                    let mut state = parent_state.clone();
                    poll_fixture_future(fork_processing::state_transition(
                        &mut state,
                        &block,
                        &params.transitions,
                        &engine,
                    ))?
                    .map_err(|error| error.to_string())?;
                    let checkpoint = full_node::finalized_checkpoint(parent_state);
                    let finalized = if checkpoint.root == [0; 32] {
                        history.get(&anchor).map(|(_, value)| value)
                    } else {
                        history.get(&checkpoint.root).map(|(_, value)| value)
                    };
                    store
                        .on_block(&state, &block, parent_state, parent_block, finalized)
                        .map_err(|error| error.to_string())?;
                    if history.len() >= 4096 {
                        return Err("fixture history exceeds bound".into());
                    }
                    history.insert(
                        block.message_root().map_err(|error| error.to_string())?,
                        (state, block),
                    );
                    Ok(())
                }
                Some("new_head") => {
                    store
                        .on_head(root(&action["head_block_root"])?)
                        .map_err(|error| error.to_string())?;
                    checks(case, &action["checks"], &store, &params, max_bytes)
                }
                _ => Err("unsupported collection event".into()),
            }
        })();
        result.map_err(|error| format!("collection step {index}: {error}"))?;
    }
    Ok(())
}
