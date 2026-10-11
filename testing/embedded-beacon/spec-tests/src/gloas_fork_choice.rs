//! Published Gloas events exercise the real private payload-branch graph.
//! Envelope fixtures explicitly assume mocked execution and data availability; no live proof is
//! credited.

use super::{
    config_override, fixed_hex, fixture_gloas_parameters, fork_choice_event_result,
    fork_choice_fixture_path, hex, poll_fixture_future, read_snappy_ssz, schema_preset, yaml, Case,
    FixtureExecutionEngine, Outcome,
};
use reth_embedded_beacon_fork_choice::{
    gloas::{artificial::ArtificialSpecStore, DataAvailability, EnvelopeError, GloasStore},
    ForkChoiceNode, PayloadStatus, StoreError, StoreParameters,
};
use reth_embedded_beacon_primitives::{Checkpoint, Root};
use reth_embedded_beacon_state_transition::ssz::{
    auxiliary::gloas::PayloadAttestationMessage,
    gloas::{
        Attestation, AttesterSlashing, BeaconBlock, BeaconState, SignedBeaconBlock,
        SignedExecutionPayloadEnvelope,
    },
};
use serde_yaml::Value;
use std::collections::BTreeSet;

pub(super) fn supports(case: &Case) -> bool {
    case.fork == "gloas" && matches!(case.runner.as_str(), "fork_choice" | "fork_choice_compliance")
}

pub(super) fn run_case(case: &Case, max_bytes: usize) -> Result<(), (Outcome, String)> {
    run(case, max_bytes).map_err(|error| (Outcome::Failed, error))
}

// Private and confined to authenticated consensus fixtures, whose DA provider is assumed true.
// Runtime deployment must supply its configured custody retrieval and real KZG verifier.
struct FixtureAvailability;
impl DataAvailability for FixtureAvailability {
    type Error = std::io::Error;
    async fn verify(&self, _root: Root, _block: &BeaconBlock) -> Result<(), Self::Error> {
        Ok(())
    }
}

// The artificial graph is a different sealed type with no conversion into a runtime Store.
// Event and observation dispatch stays private to this authenticated fixture adapter.
enum FixtureStore {
    Strict(GloasStore),
    Artificial(ArtificialSpecStore),
}
macro_rules! observe {
    ($self:expr, $method:ident $(, $argument:expr)*) => {
        match $self {
            FixtureStore::Strict(store) => store.$method($($argument),*),
            FixtureStore::Artificial(store) => store.$method($($argument),*),
        }
    };
}
impl FixtureStore {
    fn on_tick(&mut self, time_ms: u64) -> Result<(), StoreError> {
        observe!(self, on_tick, time_ms)
    }
    fn on_block_and_contents(&mut self, block: &SignedBeaconBlock) -> Result<(), StoreError> {
        observe!(self, on_block_and_contents, block)
    }
    fn on_compliance_block(&mut self, block: &SignedBeaconBlock) -> Result<(), StoreError> {
        match self {
            Self::Artificial(store) => store.on_compliance_block(block),
            Self::Strict(_) => Err(StoreError::InvalidParameters),
        }
    }
    fn on_attestation(&mut self, attestation: &Attestation) -> Result<(), StoreError> {
        observe!(self, on_attestation, attestation)
    }
    fn on_attester_slashing(&mut self, slashing: &AttesterSlashing) -> Result<(), StoreError> {
        observe!(self, on_attester_slashing, slashing)
    }
    fn on_payload_attestation_message(
        &mut self,
        message: &PayloadAttestationMessage,
    ) -> Result<(), StoreError> {
        observe!(self, on_payload_attestation_message, message)
    }
    async fn on_execution_payload_envelope(
        &mut self,
        envelope: &SignedExecutionPayloadEnvelope,
        engine: &FixtureExecutionEngine,
        availability: &FixtureAvailability,
    ) -> Result<(), EnvelopeError> {
        match self {
            Self::Strict(store) => {
                store.on_execution_payload_envelope(envelope, engine, availability).await
            }
            Self::Artificial(store) => {
                store.on_execution_payload_envelope(envelope, engine, availability).await
            }
        }
    }
    const fn time_ms(&self) -> u64 {
        observe!(self, time_ms)
    }
    const fn genesis_time_ms(&self) -> u64 {
        observe!(self, genesis_time_ms)
    }
    const fn proposer_boost_root(&self) -> Root {
        observe!(self, proposer_boost_root)
    }
    const fn checkpoints(&self) -> (Checkpoint, Checkpoint, Checkpoint, Checkpoint) {
        observe!(self, checkpoints)
    }
    fn block_state(&self, root: Root) -> Option<&BeaconState> {
        observe!(self, block_state, root)
    }
    fn get_head(&self) -> Result<ForkChoiceNode, StoreError> {
        observe!(self, get_head)
    }
    fn get_proposer_head(&self) -> Result<ForkChoiceNode, StoreError> {
        observe!(self, get_proposer_head)
    }
    fn get_filtered_node_tree(&self) -> Result<BTreeSet<ForkChoiceNode>, StoreError> {
        observe!(self, get_filtered_node_tree)
    }
    fn get_node_children(&self, node: ForkChoiceNode) -> Result<Vec<ForkChoiceNode>, StoreError> {
        observe!(self, get_node_children, node)
    }
    fn get_weight(&self, node: ForkChoiceNode) -> Result<u64, StoreError> {
        observe!(self, get_weight, node)
    }
    fn payload_timeliness_votes(&self, root: Root) -> Option<&[Option<bool>]> {
        observe!(self, payload_timeliness_votes, root)
    }
    fn payload_data_availability_votes(&self, root: Root) -> Option<&[Option<bool>]> {
        observe!(self, payload_data_availability_votes, root)
    }
}

fn run(case: &Case, max_bytes: usize) -> Result<(), String> {
    let preset = schema_preset(case)?;
    let meta = case.path.join("meta.yaml");
    let artificial = if meta.is_file() {
        let meta = yaml(&meta, max_bytes)?;
        if !meta["bls_setting"].is_null() && !matches!(meta["bls_setting"].as_u64(), Some(0..=2)) {
            return Err("fork-choice BLS metadata must be a supported fixture mode".into())
        }
        meta["bls_setting"].as_u64() == Some(2)
    } else {
        false
    };
    if case.runner == "fork_choice_compliance" && !artificial {
        return Err("Gloas compliance requires explicit artificial BLS metadata".into())
    }
    let state = BeaconState::from_ssz(
        &read_snappy_ssz(&case.path.join("anchor_state.ssz_snappy"), max_bytes)?,
        preset,
    )
    .map_err(|error| error.to_string())?;
    let block = BeaconBlock::from_ssz(
        &read_snappy_ssz(&case.path.join("anchor_block.ssz_snappy"), max_bytes)?,
        preset,
    )
    .map_err(|error| error.to_string())?;
    let mut processing = fixture_gloas_parameters(case, preset, max_bytes)?;
    // Fork-choice fixtures explicitly instantiate the named fork at genesis; only absent
    // test-network activation keys use zero. This cannot select a public network activation.
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
        .ok_or("slot duration overflow")?;
    macro_rules! setting {
        ($field:ident,$name:literal) => {
            parameters.$field = config_override(case, $name, parameters.$field, max_bytes)?
        };
    }
    setting!(gloas_attestation_due_bps, "ATTESTATION_DUE_BPS_GLOAS");
    setting!(payload_attestation_due_bps, "PAYLOAD_ATTESTATION_DUE_BPS");
    setting!(proposer_score_boost, "PROPOSER_SCORE_BOOST");
    setting!(proposer_reorg_cutoff_bps, "PROPOSER_REORG_CUTOFF_BPS");
    setting!(reorg_head_weight_threshold, "REORG_HEAD_WEIGHT_THRESHOLD");
    setting!(reorg_parent_weight_threshold, "REORG_PARENT_WEIGHT_THRESHOLD");
    setting!(reorg_max_epochs_since_finalization, "REORG_MAX_EPOCHS_SINCE_FINALIZATION");
    let mut store = if artificial {
        FixtureStore::Artificial(
            ArtificialSpecStore::get_store(state, block, parameters, processing)
                .map_err(|error| error.to_string())?,
        )
    } else {
        FixtureStore::Strict(
            GloasStore::get_store(state, block, parameters, processing)
                .map_err(|error| error.to_string())?,
        )
    };
    let metadata = yaml(&case.path.join("steps.yaml"), max_bytes)?;
    let steps = metadata.as_sequence().ok_or("fork-choice steps must be a sequence")?;
    if steps.is_empty() || steps.len() > 100_000 {
        return Err("fork-choice step count exceeds bounded budget".into())
    }
    let engine = FixtureExecutionEngine::from_case(case, max_bytes, false)?;
    let mut assertions = 0;
    for (index, step) in steps.iter().enumerate() {
        let mapping = step.as_mapping().ok_or("fork-choice step must be a mapping")?;
        if mapping.keys().any(|key| {
            !matches!(
                key.as_str(),
                Some(
                    "tick" |
                        "block" |
                        "attestation" |
                        "attester_slashing" |
                        "execution_payload" |
                        "payload_attestation_message" |
                        "checks" |
                        "valid"
                )
            )
        }) {
            return Err(format!("unsupported Gloas event field at step {index}"))
        }
        let events = [
            "tick",
            "block",
            "attestation",
            "attester_slashing",
            "execution_payload",
            "payload_attestation_message",
            "checks",
        ];
        let selected = events.iter().filter(|name| !step[**name].is_null()).collect::<Vec<_>>();
        if selected.len() != 1 {
            return Err(format!("Gloas step {index} must contain exactly one event"))
        }
        let event = *selected[0];
        let result = match event {
            "tick" => store.on_tick(
                step[event]
                    .as_u64()
                    .and_then(|value| value.checked_mul(1000))
                    .ok_or("invalid Gloas tick")?,
            ),
            "block" => {
                let bytes =
                    read_snappy_ssz(&fork_choice_fixture_path(case, &step[event])?, max_bytes)?;
                let block = SignedBeaconBlock::from_ssz(&bytes, preset)
                    .map_err(|error| error.to_string())?;
                if case.runner == "fork_choice_compliance" {
                    store.on_compliance_block(&block)
                } else {
                    store.on_block_and_contents(&block)
                }
            }
            "attestation" => {
                let bytes =
                    read_snappy_ssz(&fork_choice_fixture_path(case, &step[event])?, max_bytes)?;
                let value =
                    Attestation::from_ssz(&bytes, preset).map_err(|error| error.to_string())?;
                store.on_attestation(&value)
            }
            "attester_slashing" => {
                let bytes =
                    read_snappy_ssz(&fork_choice_fixture_path(case, &step[event])?, max_bytes)?;
                let value = AttesterSlashing::from_ssz(&bytes, preset)
                    .map_err(|error| error.to_string())?;
                store.on_attester_slashing(&value)
            }
            "payload_attestation_message" => {
                let bytes =
                    read_snappy_ssz(&fork_choice_fixture_path(case, &step[event])?, max_bytes)?;
                let value = PayloadAttestationMessage::from_ssz(&bytes, preset)
                    .map_err(|error| error.to_string())?;
                store.on_payload_attestation_message(&value)
            }
            "execution_payload" => {
                let bytes =
                    read_snappy_ssz(&fork_choice_fixture_path(case, &step[event])?, max_bytes)?;
                let envelope = SignedExecutionPayloadEnvelope::from_ssz(&bytes, preset)
                    .map_err(|error| error.to_string())?;
                let result = poll_fixture_future(store.on_execution_payload_envelope(
                    &envelope,
                    &engine,
                    &FixtureAvailability,
                ))?;
                let expected = if step["valid"].is_null() {
                    true
                } else {
                    step["valid"].as_bool().ok_or("Gloas validity expectation must be boolean")?
                };
                envelope_result(result, expected)
                    .map_err(|error| format!("Gloas envelope step {index}: {error}"))?;
                continue
            }
            "checks" => {
                assertions += 1;
                checks(&store, &step[event])
                    .map_err(|error| format!("Gloas assertion step {index}: {error}"))?;
                continue
            }
            _ => return Err("unknown Gloas event".into()),
        };
        fork_choice_event_result(result, step)
            .map_err(|error| format!("Gloas event step {index}: {error}"))?;
    }
    if assertions == 0 {
        return Err("fork-choice fixture has no actual assertions".into())
    }
    Ok(())
}

fn envelope_result(result: Result<(), EnvelopeError>, expected: bool) -> Result<(), String> {
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
fn payload_status(value: &Value) -> Result<PayloadStatus, String> {
    match value.as_u64() {
        Some(0) => Ok(PayloadStatus::Empty),
        Some(1) => Ok(PayloadStatus::Full),
        Some(2) => Ok(PayloadStatus::Pending),
        _ => Err("invalid Gloas payload status".into()),
    }
}
fn checks(store: &FixtureStore, checks: &Value) -> Result<(), String> {
    let mapping = checks.as_mapping().ok_or("Gloas checks must be a mapping")?;
    if mapping.is_empty() ||
        mapping.keys().any(|key| {
            !matches!(
                key.as_str(),
                Some(
                    "head" |
                        "time" |
                        "genesis_time" |
                        "justified_checkpoint" |
                        "finalized_checkpoint" |
                        "proposer_boost_root" |
                        "get_proposer_head" |
                        "payload_timeliness_vote" |
                        "payload_data_availability_vote" |
                        "viable_for_head_roots_and_weights"
                )
            )
        })
    {
        return Err("unsupported Gloas assertion field".into())
    }
    for (name, actual) in [("time", store.time_ms()), ("genesis_time", store.genesis_time_ms())] {
        if !checks[name].is_null() &&
            checks[name].as_u64().and_then(|value| value.checked_mul(1000)) != Some(actual)
        {
            return Err(format!("{name} differs"))
        }
    }
    if !checks["head"].is_null() {
        let actual = store.get_head().map_err(|error| error.to_string())?;
        let expected = &checks["head"];
        if (!expected["root"].is_null() && fixed_hex::<32>(&expected["root"])? != actual.root) ||
            (!expected["slot"].is_null() &&
                expected["slot"].as_u64() !=
                    store.block_state(actual.root).map(|state| state.slot)) ||
            (!expected["payload_status"].is_null() &&
                Some(payload_status(&expected["payload_status"])?) != actual.payload_status)
        {
            return Err(format!(
                "head differs: received root {}, status {:?}",
                hex(&actual.root),
                actual.payload_status
            ))
        }
    }
    if !checks["get_proposer_head"].is_null() {
        let actual = store.get_proposer_head().map_err(|error| error.to_string())?;
        let expected = &checks["get_proposer_head"];
        if expected.is_string() {
            if fixed_hex::<32>(expected)? != actual.root {
                return Err("proposer head differs".into())
            }
        } else if fixed_hex::<32>(&expected["root"])? != actual.root ||
            Some(payload_status(&expected["payload_status"])?) != actual.payload_status
        {
            return Err("proposer head differs".into())
        }
    }
    for (name, actual) in [
        ("justified_checkpoint", store.checkpoints().0),
        ("finalized_checkpoint", store.checkpoints().1),
    ] {
        if !checks[name].is_null() &&
            (checks[name]["epoch"].as_u64() != Some(actual.epoch) ||
                fixed_hex::<32>(&checks[name]["root"])? != actual.root)
        {
            return Err(format!("{name} differs"))
        }
    }
    if !checks["proposer_boost_root"].is_null() &&
        fixed_hex::<32>(&checks["proposer_boost_root"])? != store.proposer_boost_root()
    {
        return Err("proposer boost differs".into())
    }
    for (name, timely) in
        [("payload_timeliness_vote", true), ("payload_data_availability_vote", false)]
    {
        let expected = &checks[name];
        if !expected.is_null() {
            let root = fixed_hex::<32>(&expected["block_root"])?;
            let actual = if timely {
                store.payload_timeliness_votes(root)
            } else {
                store.payload_data_availability_votes(root)
            }
            .ok_or("unknown PTC root")?;
            let expected = expected["votes"]
                .as_sequence()
                .ok_or("PTC expected votes must be a sequence")?
                .iter()
                .map(|vote| {
                    if vote.is_null() {
                        Ok(None)
                    } else {
                        vote.as_bool().map(Some).ok_or("PTC vote must be boolean or null")
                    }
                })
                .collect::<Result<Vec<_>, _>>()?;
            if actual != expected {
                return Err(format!("{name} positions differ"))
            }
        }
    }
    if !checks["viable_for_head_roots_and_weights"].is_null() {
        let viable = store.get_filtered_node_tree().map_err(|error| error.to_string())?;
        let mut actual = BTreeSet::new();
        for &node in &viable {
            if store
                .get_node_children(node)
                .map_err(|error| error.to_string())?
                .iter()
                .all(|child| !viable.contains(child))
            {
                actual.insert((
                    node.root,
                    node.payload_status,
                    store.get_weight(node).map_err(|error| error.to_string())?,
                ));
            }
        }
        let mut expected = BTreeSet::new();
        for entry in checks["viable_for_head_roots_and_weights"]
            .as_sequence()
            .ok_or("viable weighted nodes must be a sequence")?
        {
            let node = ForkChoiceNode {
                root: fixed_hex::<32>(&entry["root"])?,
                payload_status: Some(payload_status(&entry["payload_status"])?),
            };
            if !expected.insert((
                node.root,
                node.payload_status,
                entry["weight"].as_u64().ok_or("invalid viable weight")?,
            )) {
                return Err("duplicate viable weighted node".into())
            }
        }
        if actual != expected {
            return Err(format!(
                "viable weighted leaf set differs: expected {expected:?}, received {actual:?}"
            ))
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use reth_embedded_beacon_primitives::Fork;

    #[test]
    fn unavailable_verification_and_resource_exhaustion_do_not_satisfy_invalid_fixture() {
        assert!(envelope_result(Err(StoreError::ResourceLimit.into()), false).is_err());
        assert!(envelope_result(Err(StoreError::Overflow.into()), false).is_err());
        assert!(
            envelope_result(Err(StoreError::ValidationUnavailable(Fork::Gloas).into()), false,)
                .is_err()
        );
        assert!(envelope_result(
            Err(EnvelopeError::Verification("invalid signature".into())),
            false
        )
        .is_ok());
        assert!(envelope_result(Ok(()), false).is_err());
        assert!(envelope_result(Err(StoreError::UnknownRoot.into()), true).is_err());
    }
}
