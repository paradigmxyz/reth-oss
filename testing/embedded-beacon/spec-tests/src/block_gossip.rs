//! Original block gossip fixtures use strict production propagation and signed setup replay.
//! Failed blocks, declared execution status/finality and anchor payloads remain isolated
//! observations. Execution callbacks used by synthetic setup blocks are explicitly reported.

use super::{
    fixture_transition_parameters, fork_choice_availability::FixtureAvailability,
    fork_choice_fixture_path, poll_fixture_future, read_snappy_ssz, schema_fork_name,
    schema_preset, yaml, Case, FixtureExecutionEngine, Outcome,
};
use reth_embedded_beacon_fork_choice::{
    gloas::spec_only::SpecStore,
    gossip_view::GossipView,
    store::{
        block_gossip::{self, GossipBlockError, SeenBeaconBlocks},
        strict_gossip_fixture::StrictGossipStore,
    },
    ExecutionStatus, StoreParameters,
};
use reth_embedded_beacon_primitives::{root_from_hex, Checkpoint, Fork, Root};
use reth_embedded_beacon_state_transition::{
    fork_processing::TransitionParameters,
    ssz::{gloas, BeaconBlock, BeaconState, Preset, SignedBeaconBlock},
};
use serde_yaml::Value;
use std::collections::BTreeMap;

pub(super) fn supports(case: &Case) -> bool {
    case.runner == "networking" &&
        case.handler == "gossip_beacon_block" &&
        super::FORKS.contains(&case.fork.as_str())
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
    if meta["topic"].as_str() != Some("beacon_block") {
        return Err("block gossip topic differs from handler".into())
    }
    if !meta["bls_setting"].is_null() && meta["bls_setting"].as_u64() != Some(1) {
        return Err("block gossip requires real fixture signatures".into())
    }
    let store = load_fixture_store(case, &meta, max_bytes)?;
    let mut seen = store.new_seen()?;
    let messages =
        meta["messages"].as_sequence().ok_or("block gossip messages must be a sequence")?;
    if messages.is_empty() || messages.len() > 4096 {
        return Err("block gossip message budget exceeded".into())
    }
    for (index, entry) in messages.iter().enumerate() {
        fields(entry, &["message", "current_time_ms", "expected", "reason"])?;
        let signed = read_block(case, &entry["message"], fork, preset, max_bytes)?;
        let time = unsigned(&entry["current_time_ms"])?;
        let result = store.validate(&mut seen, &signed, time)?;
        let (actual, reason) = match result {
            Ok(()) => ("valid", None),
            Err(GossipBlockError::Ignore(reason)) => ("ignore", Some(reason)),
            Err(GossipBlockError::Reject(reason)) => ("reject", Some(reason)),
            Err(error) => {
                return Err(format!("block gossip event {index} infrastructure failure: {error}"))
            }
        };
        let expected =
            entry["expected"].as_str().ok_or("block gossip expected result must be a string")?;
        let expected_reason = if entry["reason"].is_null() {
            None
        } else {
            Some(entry["reason"].as_str().ok_or("block gossip reason must be a string")?)
        };
        if actual != expected || reason != expected_reason {
            return Err(format!("block gossip event {index}: expected {expected}/{expected_reason:?}, received {actual}/{reason:?}"))
        }
    }
    Ok(())
}

/// Load exact authenticated setup through the strict production transitions.
/// Declared failed blocks, finality, payloads and status remain isolated observations.
pub(super) fn load_fixture_store(
    case: &Case,
    meta: &Value,
    max_bytes: usize,
) -> Result<FixtureStore, String> {
    let preset = schema_preset(case)?;
    let fork = schema_fork_name(&case.fork)?;
    if !meta["bls_setting"].is_null() && meta["bls_setting"].as_u64() != Some(1) {
        return Err("strict gossip setup requires real fixture signatures".into())
    }
    let blocks = meta["blocks"].as_sequence().ok_or("block gossip blocks must be a sequence")?;
    if blocks.is_empty() || blocks.len() > 4096 {
        return Err("block gossip setup budget exceeded".into())
    }
    let state = BeaconState::from_ssz(
        &read_snappy_ssz(&case.path.join("state.ssz_snappy"), max_bytes)?,
        fork,
        preset,
    )
    .map_err(|error| error.to_string())?;
    let anchor = read_block(case, &blocks[0]["block"], fork, preset, max_bytes)?;
    let header = anchor.header().map_err(|error| error.to_string())?;
    if header.slot != state.slot() ||
        header.state_root != state.hash_tree_root().map_err(|error| error.to_string())?
    {
        return Err("block gossip complete anchor and state do not match".into())
    }
    if flag(&blocks[0], "failed")? {
        return Err("block gossip anchor cannot be a failed observation".into())
    }
    let transition =
        fixture_transition_parameters(case, preset, fork, state.fork_info().epoch, max_bytes)?;
    let inherited = transition.gloas.fulu.electra.deneb.capella.bellatrix.altair;
    let mut parameters = StoreParameters::for_preset(preset);
    parameters.phase0 = inherited.phase0;
    parameters.altair_fork_version = inherited.fork_version;
    parameters.inactivity_score_bias = inherited.inactivity_score_bias;
    parameters.inactivity_score_recovery_rate = inherited.inactivity_score_recovery_rate;
    parameters.slot_duration_ms = transition
        .gloas
        .fulu
        .electra
        .deneb
        .capella
        .bellatrix
        .seconds_per_slot
        .checked_mul(1000)
        .ok_or("block gossip clock overflow")?;
    let anchor_root = anchor.message_root().map_err(|error| error.to_string())?;
    let mut store = match (state, anchor) {
        (BeaconState::Gloas(state, _), SignedBeaconBlock::Gloas(anchor, _)) => {
            FixtureStore::Gloas(Box::new(
                SpecStore::get_store(state, anchor.message, parameters, transition.gloas.clone())
                    .map_err(|error| error.to_string())?,
            ))
        }
        (state, anchor) if fork != Fork::Gloas => {
            let mut store = StrictGossipStore::get_store(state, into_message(anchor), parameters)
                .map_err(|error| error.to_string())?;
            store
                .set_transition_parameters(transition.clone())
                .map_err(|error| error.to_string())?;
            FixtureStore::Classic(Box::new(store))
        }
        _ => return Err("block gossip anchor schema mismatch".into()),
    };
    let engine = FixtureExecutionEngine::from_case(case, max_bytes, false)?;
    let mut setup_roots = BTreeMap::new();
    for (index, entry) in blocks.iter().enumerate() {
        fields(entry, &["block", "failed", "payload_status", "payload"])?;
        let failed = flag(entry, "failed")?;
        let root = if index == 0 {
            anchor_root
        } else {
            let block = read_block(case, &entry["block"], fork, preset, max_bytes)?;
            let root = block.message_root().map_err(|error| error.to_string())?;
            let slot = block.header().map_err(|error| error.to_string())?.slot;
            if failed {
                store.observe_failed(block)?;
            } else {
                store.import_setup(
                    case,
                    block,
                    slot,
                    entry,
                    &engine,
                    &transition,
                    preset,
                    max_bytes,
                )?;
            }
            root
        };
        let name =
            entry["block"].as_str().ok_or("block gossip setup reference must be a string")?;
        setup_roots.insert(name.to_owned(), root);
        if !entry["payload_status"].is_null() {
            let status = match entry["payload_status"].as_str() {
                Some("VALID") => ExecutionStatus::Valid,
                Some("INVALIDATED") => ExecutionStatus::Invalid,
                Some("NOT_VALIDATED") => ExecutionStatus::Optimistic,
                _ => return Err("unknown block gossip payload status declaration".into()),
            };
            store.declare_payload_status(root, status)?;
        }
        if !entry["payload"].is_null() {
            if failed {
                return Err("failed block cannot declare a verified anchor payload".into())
            }
            let envelope = gloas::SignedExecutionPayloadEnvelope::from_ssz(
                &read_snappy_ssz(&fork_choice_fixture_path(case, &entry["payload"])?, max_bytes)?,
                preset,
            )
            .map_err(|error| error.to_string())?;
            if envelope.message.beacon_block_root != root {
                return Err("asserted payload does not reference its exact declared block".into())
            }
            store.assert_payload(&envelope)?;
        }
    }
    if !meta["finalized_checkpoint"].is_null() {
        store.declare_finalized(declared_finalized_checkpoint(
            &meta["finalized_checkpoint"],
            &setup_roots,
        )?)?;
    }
    Ok(store)
}

pub(super) enum FixtureStore {
    Classic(Box<StrictGossipStore>),
    Gloas(Box<SpecStore>),
}
impl FixtureStore {
    fn observe_failed(&mut self, signed: SignedBeaconBlock) -> Result<(), String> {
        match (&mut *self, signed) {
            (Self::Gloas(store), SignedBeaconBlock::Gloas(block, _)) => store
                .fixture_observe_block_without_state(block.message)
                .map(|_| ())
                .map_err(|error| error.to_string()),
            (Self::Classic(store), block) if block.schema_fork() != Fork::Gloas => store
                .fixture_observe_block_without_state(into_message(block))
                .map(|_| ())
                .map_err(|error| error.to_string()),
            _ => Err("failed block observation schema mismatch".into()),
        }
    }
    #[allow(clippy::too_many_arguments)]
    fn import_setup(
        &mut self,
        case: &Case,
        signed: SignedBeaconBlock,
        slot: u64,
        entry: &Value,
        engine: &FixtureExecutionEngine,
        transition: &TransitionParameters,
        preset: Preset,
        max_bytes: usize,
    ) -> Result<(), String> {
        let duration = match self {
            Self::Classic(store) => store.slot_duration_ms(),
            Self::Gloas(store) => store.slot_duration_ms(),
        };
        let genesis = match self {
            Self::Classic(store) => store.genesis_time_ms(),
            Self::Gloas(store) => store.genesis_time_ms(),
        };
        let time = slot
            .checked_mul(duration)
            .and_then(|value| value.checked_add(genesis))
            .ok_or("block gossip setup clock overflow")?;
        match (self, signed) {
            (Self::Gloas(store), SignedBeaconBlock::Gloas(block, _)) => {
                store.on_tick(time.max(store.time_ms())).map_err(|error| error.to_string())?;
                store
                    .on_block_and_contents(&block)
                    .map_err(|error| format!("strict Gloas setup import: {error}"))
            }
            (Self::Classic(store), block) if block.schema_fork() != Fork::Gloas => {
                store.on_tick(time.max(store.time_ms())).map_err(|error| error.to_string())?;
                let availability = FixtureAvailability::new(
                    case,
                    entry,
                    preset,
                    transition.gloas.fulu.clone(),
                    max_bytes,
                );
                poll_fixture_future(store.on_block_and_contents_with_engine(
                    &block,
                    engine,
                    &availability,
                ))?
                .map_err(|error| format!("strict classic setup import: {error}"))
            }
            _ => Err("setup block schema mismatch".into()),
        }
    }
    fn declare_payload_status(
        &mut self,
        root: [u8; 32],
        status: ExecutionStatus,
    ) -> Result<(), String> {
        match self {
            Self::Classic(store) => {
                store.fixture_set_payload_status(root, status).map_err(|error| error.to_string())
            }
            Self::Gloas(store) => {
                store.fixture_set_payload_status(root, status).map_err(|error| error.to_string())
            }
        }
    }
    fn assert_payload(
        &mut self,
        signed: &gloas::SignedExecutionPayloadEnvelope,
    ) -> Result<(), String> {
        match self {
            Self::Gloas(store) => store
                .fixture_assert_verified_payload_observation(signed)
                .map_err(|error| error.to_string()),
            Self::Classic(_) => Err("classic block cannot declare a separate Gloas payload".into()),
        }
    }
    fn declare_finalized(&mut self, checkpoint: Checkpoint) -> Result<(), String> {
        match self {
            Self::Classic(store) => store
                .fixture_set_finalized_checkpoint(checkpoint)
                .map_err(|error| error.to_string()),
            Self::Gloas(store) => store
                .fixture_set_gossip_finalized_checkpoint(checkpoint)
                .map_err(|error| error.to_string()),
        }
    }
    fn new_seen(&self) -> Result<SeenBeaconBlocks, String> {
        match self {
            Self::Classic(store) => SeenBeaconBlocks::new(store.as_ref(), 262_144),
            Self::Gloas(store) => SeenBeaconBlocks::new_gloas(store.as_ref(), 262_144),
        }
        .map_err(|error| error.to_string())
    }
    fn validate(
        &self,
        seen: &mut SeenBeaconBlocks,
        signed: &SignedBeaconBlock,
        time: u64,
    ) -> Result<Result<(), GossipBlockError>, String> {
        Ok(match (self, signed) {
            (Self::Classic(store), block) if block.schema_fork() != Fork::Gloas => {
                block_gossip::validate(store.as_ref(), seen, block, time)
            }
            (Self::Gloas(store), SignedBeaconBlock::Gloas(block, _)) => {
                block_gossip::validate_gloas(store.as_ref(), seen, block, time)
            }
            _ => return Err("candidate block schema mismatch".into()),
        })
    }
}

/// Decode the two published checkpoint encodings into an isolated graph observation.
/// Named blocks must be exact signed setup references already decoded by the strict loader.
/// An explicit root may be unknown, as required by the negative ancestor fixtures.
fn declared_finalized_checkpoint(
    value: &Value,
    setup_roots: &BTreeMap<String, Root>,
) -> Result<Checkpoint, String> {
    fields(value, &["epoch", "root", "block"])?;
    let root = match (value.get("root").is_some(), value.get("block").is_some()) {
        (true, false) => root_from_hex(
            value["root"].as_str().ok_or("block gossip finalized root must be a string")?,
        )
        .map_err(|error| error.to_string())?,
        (false, true) => {
            let name = value["block"]
                .as_str()
                .ok_or("block gossip finalized block reference must be a string")?;
            *setup_roots.get(name).ok_or("block gossip finalized block is not in signed setup")?
        }
        _ => {
            return Err("block gossip finalized checkpoint requires exactly one root or block".into())
        }
    };
    Ok(Checkpoint { epoch: unsigned(&value["epoch"])?, root })
}

fn fields(value: &Value, allowed: &[&str]) -> Result<(), String> {
    let mapping = value.as_mapping().ok_or("block gossip metadata must be a mapping")?;
    if mapping.keys().any(|key| !key.as_str().is_some_and(|key| allowed.contains(&key))) {
        return Err("unknown block gossip metadata field".into())
    }
    Ok(())
}
fn flag(value: &Value, name: &str) -> Result<bool, String> {
    if value[name].is_null() {
        Ok(false)
    } else {
        value[name].as_bool().ok_or_else(|| format!("block gossip {name} must be boolean"))
    }
}
fn unsigned(value: &Value) -> Result<u64, String> {
    value.as_u64().ok_or_else(|| "block gossip scalar must be unsigned".into())
}
fn read_block(
    case: &Case,
    value: &Value,
    fork: Fork,
    preset: Preset,
    max_bytes: usize,
) -> Result<SignedBeaconBlock, String> {
    SignedBeaconBlock::from_ssz(
        &read_snappy_ssz(&fork_choice_fixture_path(case, value)?, max_bytes)?,
        fork,
        preset,
    )
    .map_err(|error| error.to_string())
}
fn into_message(signed: SignedBeaconBlock) -> BeaconBlock {
    match signed {
        SignedBeaconBlock::Phase0(block, preset) => BeaconBlock::Phase0(block.message, preset),
        SignedBeaconBlock::Altair(block, preset) => BeaconBlock::Altair(block.message, preset),
        SignedBeaconBlock::Bellatrix(block, preset) => {
            BeaconBlock::Bellatrix(block.message, preset)
        }
        SignedBeaconBlock::Capella(block, preset) => BeaconBlock::Capella(block.message, preset),
        SignedBeaconBlock::Deneb(block, preset) => BeaconBlock::Deneb(block.message, preset),
        SignedBeaconBlock::Electra(block, preset) => BeaconBlock::Electra(block.message, preset),
        SignedBeaconBlock::Fulu(block, preset) => BeaconBlock::Fulu(block.message, preset),
        SignedBeaconBlock::Gloas(block, preset) => BeaconBlock::Gloas(block.message, preset),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn published_finality_encodings_require_exactly_one_checked_reference() {
        let roots = BTreeMap::from([("block_anchor".to_owned(), [9; 32])]);
        let block: Value = serde_yaml::from_str("epoch: 1\nblock: block_anchor\n").unwrap();
        assert_eq!(
            declared_finalized_checkpoint(&block, &roots).unwrap(),
            Checkpoint { epoch: 1, root: [9; 32] },
        );
        let root = format!("epoch: 0\nroot: '0x{}'\n", "ab".repeat(32));
        let explicit: Value = serde_yaml::from_str(&root).unwrap();
        assert_eq!(
            declared_finalized_checkpoint(&explicit, &roots).unwrap(),
            Checkpoint { epoch: 0, root: [0xab; 32] },
        );
        for invalid in [
            "epoch: 1\n",
            "epoch: 1\nblock: unknown\n",
            "epoch: 1\nblock: block_anchor\nextra: true\n",
            "epoch: -1\nblock: block_anchor\n",
            "epoch: 1\nblock: 5\n",
            "epoch: 1\nblock: block_anchor\nroot: null\n",
            "epoch: 1\nblock: null\n",
        ] {
            let value: Value = serde_yaml::from_str(invalid).unwrap();
            assert!(declared_finalized_checkpoint(&value, &roots).is_err());
        }
        let both: Value = serde_yaml::from_str(&(root + "block: block_anchor\n")).unwrap();
        assert!(declared_finalized_checkpoint(&both, &roots).is_err());
    }
}
