//! Fulu column gossip fixtures run strict production signatures, ancestry and real KZG.
//! Declared graph/header observations use an isolated fixed-BLS-off graph and cache.

use super::{
    config_override, fixed_hex, fixture_transition_parameters,
    fork_choice_availability::FixtureAvailability, fork_choice_fixture_path, poll_fixture_future,
    read_snappy_ssz, schema_preset, yaml, Case, FixtureExecutionEngine, Outcome,
};
use c_kzg::{ethereum_kzg_settings, KzgSettings};
use reth_embedded_beacon_fork_choice::{SpecStore, Store, StoreParameters};
use reth_embedded_beacon_node::{
    fulu_columns::{spec_only::SpecSeenFuluColumns, SeenFuluColumns},
    gloas_columns::ColumnGossipError,
};
use reth_embedded_beacon_primitives::{Checkpoint, Fork, Root};
use reth_embedded_beacon_state_transition::ssz::{
    auxiliary::fulu::{
        DataColumnSidecar, PartialDataColumnGroupID, PartialDataColumnHeader,
        PartialDataColumnSidecar,
    },
    BeaconState, SignedBeaconBlock,
};
use serde_yaml::Value;

pub(super) fn supports(case: &Case) -> bool {
    case.fork == "fulu" &&
        case.runner == "networking" &&
        matches!(
            case.handler.as_str(),
            "gossip_data_column_sidecar" | "gossip_partial_data_column_sidecar"
        )
}

pub(super) fn uses_execution_mock(case: &Case, max_bytes: usize) -> bool {
    supports(case) &&
        yaml(&case.path.join("meta.yaml"), max_bytes).is_ok_and(|meta| {
            meta["blocks"].as_sequence().is_some_and(|blocks| {
                blocks.iter().skip(1).any(|entry| {
                    entry["pending"].as_bool() != Some(true) &&
                        entry["failed"].as_bool() != Some(true)
                })
            })
        })
}

pub(super) fn uses_artificial_context(case: &Case, max_bytes: usize) -> bool {
    supports(case) &&
        yaml(&case.path.join("meta.yaml"), max_bytes).is_ok_and(|meta| artificial_context(&meta))
}

fn artificial_context(meta: &Value) -> bool {
    !meta["finalized_checkpoint"].is_null() ||
        meta["bls_setting"].as_u64() == Some(2) ||
        !meta["seen_partial_data_column_headers"].is_null() ||
        meta["blocks"].as_sequence().is_some_and(|blocks| {
            blocks.iter().any(|entry| entry["failed"].as_bool() == Some(true))
        })
}

pub(super) fn run_case(case: &Case, max_bytes: usize) -> Result<(), (Outcome, String)> {
    let meta =
        yaml(&case.path.join("meta.yaml"), max_bytes).map_err(|error| (Outcome::Failed, error))?;
    run(case, max_bytes, &meta).map_err(|error| (Outcome::Failed, error))
}

fn run(case: &Case, max_bytes: usize, meta: &Value) -> Result<(), String> {
    let preset = schema_preset(case)?;
    let blocks = meta["blocks"].as_sequence().ok_or("column fixture blocks must be a sequence")?;
    if blocks.is_empty() || blocks.len() > 4096 {
        return Err("column fixture block budget exceeded".into());
    }
    let state = BeaconState::from_ssz(
        &read_snappy_ssz(&case.path.join("state.ssz_snappy"), max_bytes)?,
        Fork::Fulu,
        preset,
    )
    .map_err(|error| error.to_string())?;
    let anchor = SignedBeaconBlock::from_ssz(
        &read_snappy_ssz(&fork_choice_fixture_path(case, &blocks[0]["block"])?, max_bytes)?,
        Fork::Fulu,
        preset,
    )
    .map_err(|error| error.to_string())?;
    let epoch = config_override(case, "FULU_FORK_EPOCH", 0, max_bytes)?;
    let processing = fixture_transition_parameters(case, preset, Fork::Fulu, epoch, max_bytes)?;
    let inherited = processing.gloas.fulu.electra.deneb.capella.bellatrix.altair;
    let mut parameters = StoreParameters::for_preset(preset);
    parameters.phase0 = inherited.phase0;
    parameters.altair_fork_version = inherited.fork_version;
    parameters.inactivity_score_bias = inherited.inactivity_score_bias;
    parameters.inactivity_score_recovery_rate = inherited.inactivity_score_recovery_rate;
    parameters.slot_duration_ms = processing
        .gloas
        .fulu
        .electra
        .deneb
        .capella
        .bellatrix
        .seconds_per_slot
        .checked_mul(1000)
        .ok_or("column fixture clock overflow")?;
    let artificial = artificial_context(meta);
    let mut store = if artificial {
        FixtureGraph::Artificial(
            SpecStore::get_store(state, anchor.message(), parameters)
                .map_err(|error| error.to_string())?,
        )
    } else {
        FixtureGraph::Runtime(
            Store::get_store(state, anchor.message(), parameters)
                .map_err(|error| error.to_string())?,
        )
    };
    store.set_transition_parameters(processing.clone())?;
    let engine = FixtureExecutionEngine::from_case(case, max_bytes, false)?;
    let mut seen = if artificial {
        FixtureSeen::Artificial(
            SpecSeenFuluColumns::new(131_072, 128, 8 * 1024 * 1024, 1024)
                .map_err(|error| error.to_string())?,
        )
    } else {
        FixtureSeen::Runtime(
            SeenFuluColumns::new(131_072, 128, 8 * 1024 * 1024, 1024)
                .map_err(|error| error.to_string())?,
        )
    };
    for entry in blocks.iter().skip(1) {
        let block = SignedBeaconBlock::from_ssz(
            &read_snappy_ssz(&fork_choice_fixture_path(case, &entry["block"])?, max_bytes)?,
            Fork::Fulu,
            preset,
        )
        .map_err(|error| error.to_string())?;
        seen.observe_block(block.message_root().map_err(|error| error.to_string())?)
            .map_err(|error| error.to_string())?;
        if entry["pending"].as_bool() == Some(true) {
            continue;
        }
        if entry["failed"].as_bool() == Some(true) {
            store.observe_without_state(block.message())?;
            continue;
        }
        let time = store
            .genesis_time_ms()
            .checked_add(
                block
                    .header()
                    .map_err(|error| error.to_string())?
                    .slot
                    .checked_mul(store.slot_duration_ms())
                    .ok_or("column fixture block clock overflow")?,
            )
            .ok_or("column fixture block clock overflow")?;
        store.on_tick(time)?;
        let availability =
            FixtureAvailability::new(case, entry, preset, processing.gloas.fulu.clone(), max_bytes);
        store.import_block(&block, &engine, &availability)?;
    }
    if let Some(checkpoint) = meta.get("finalized_checkpoint") {
        let epoch = unsigned(&checkpoint["epoch"])?;
        let root = if checkpoint["root"].as_str().is_some() {
            fixed_hex::<32>(&checkpoint["root"])?
        } else {
            SignedBeaconBlock::from_ssz(
                &read_snappy_ssz(
                    &fork_choice_fixture_path(case, &checkpoint["block"])?,
                    max_bytes,
                )?,
                Fork::Fulu,
                preset,
            )
            .map_err(|error| error.to_string())?
            .message_root()
            .map_err(|error| error.to_string())?
        };
        store.set_finalized(Checkpoint { epoch, root })?;
    }
    if let Some(headers) = meta["seen_partial_data_column_headers"].as_sequence() {
        if headers.len() > 128 {
            return Err("declared partial header count exceeds fixture budget".into())
        }
        for entry in headers {
            let root = fixed_hex::<32>(&entry["block_root"])?;
            let header = PartialDataColumnHeader::from_ssz(
                &read_snappy_ssz(&fork_choice_fixture_path(case, &entry["header"])?, max_bytes)?,
                preset,
            )
            .map_err(|error| error.to_string())?;
            seen.preload_header(root, header)?;
        }
    }
    let messages =
        meta["messages"].as_sequence().ok_or("column fixture messages must be a sequence")?;
    if messages.is_empty() || messages.len() > 100_000 {
        return Err("column fixture message budget exceeded".into());
    }
    for (index, message) in messages.iter().enumerate() {
        let bytes =
            read_snappy_ssz(&fork_choice_fixture_path(case, &message["message"])?, max_bytes)?;
        let actual = if case.handler == "gossip_data_column_sidecar" {
            let sidecar =
                DataColumnSidecar::from_ssz(&bytes, preset).map_err(|error| error.to_string())?;
            seen.validate_complete(
                &store,
                &sidecar,
                unsigned(&message["current_time_ms"])?,
                unsigned(&message["subnet_id"])?,
                ethereum_kzg_settings(0),
            )
        } else {
            let sidecar = PartialDataColumnSidecar::from_ssz(&bytes, preset)
                .map_err(|error| error.to_string())?;
            let group = PartialDataColumnGroupID::from_ssz(
                &read_snappy_ssz(
                    &fork_choice_fixture_path(case, &message["group_id"])?,
                    max_bytes,
                )?,
                preset,
            )
            .map_err(|error| error.to_string())?;
            seen.validate_partial(
                &store,
                &sidecar,
                unsigned(&message["current_time_ms"])?,
                &group,
                unsigned(&message["column_index"])?,
                ethereum_kzg_settings(0),
            )
        };
        let expected = message["expected"].as_str().ok_or("column expected category missing")?;
        let category = match actual {
            Ok(()) => "valid",
            Err(ColumnGossipError::Ignore(_)) => "ignore",
            Err(ColumnGossipError::Reject(_)) => "reject",
            Err(error) => return Err(error.to_string()),
        };
        if category != expected {
            return Err(format!("column message {index}: expected {expected},received {category}"));
        }
    }
    Ok(())
}

fn unsigned(value: &Value) -> Result<u64, String> {
    value.as_u64().ok_or_else(|| "column fixture unsigned metadata missing".into())
}

enum FixtureGraph {
    Runtime(Store),
    Artificial(SpecStore),
}

impl FixtureGraph {
    fn set_transition_parameters(
        &mut self,
        parameters: reth_embedded_beacon_state_transition::fork_processing::TransitionParameters,
    ) -> Result<(), String> {
        match self {
            Self::Runtime(store) => store.set_transition_parameters(parameters),
            Self::Artificial(store) => store.set_transition_parameters(parameters),
        }
        .map_err(|error| error.to_string())
    }
    const fn genesis_time_ms(&self) -> u64 {
        match self {
            Self::Runtime(store) => store.genesis_time_ms(),
            Self::Artificial(store) => store.genesis_time_ms(),
        }
    }
    const fn slot_duration_ms(&self) -> u64 {
        match self {
            Self::Runtime(store) => store.slot_duration_ms(),
            Self::Artificial(store) => store.slot_duration_ms(),
        }
    }
    fn on_tick(&mut self, time: u64) -> Result<(), String> {
        match self {
            Self::Runtime(store) => store.on_tick(time),
            Self::Artificial(store) => store.on_tick(time),
        }
        .map_err(|error| error.to_string())
    }
    fn import_block(
        &mut self,
        block: &SignedBeaconBlock,
        engine: &FixtureExecutionEngine,
        availability: &FixtureAvailability,
    ) -> Result<(), String> {
        let result =
            match self {
                Self::Runtime(store) => poll_fixture_future(
                    store.on_block_and_contents_with_engine(block, engine, availability),
                )?,
                Self::Artificial(store) => poll_fixture_future(
                    store.on_block_and_contents_with_engine(block, engine, availability),
                )?,
            };
        result.map_err(|error| format!("fixture block import: {error}"))
    }
    fn observe_without_state(
        &mut self,
        block: reth_embedded_beacon_state_transition::ssz::BeaconBlock,
    ) -> Result<(), String> {
        let Self::Artificial(store) = self else {
            return Err("declared failed graph context requires isolated store".into())
        };
        store.fixture_observe_block_without_state(block).map_err(|error| error.to_string())
    }
    fn set_finalized(&mut self, checkpoint: Checkpoint) -> Result<(), String> {
        let Self::Artificial(store) = self else {
            return Err("declared checkpoint context requires isolated store".into())
        };
        store.fixture_set_finalized_checkpoint(checkpoint).map_err(|error| error.to_string())
    }
}

enum FixtureSeen {
    Runtime(SeenFuluColumns),
    Artificial(SpecSeenFuluColumns),
}

impl FixtureSeen {
    fn observe_block(&mut self, root: Root) -> Result<(), ColumnGossipError> {
        match self {
            Self::Runtime(seen) => seen.observe_block(root),
            Self::Artificial(seen) => seen.observe_block(root),
        }
    }
    fn preload_header(
        &mut self,
        root: Root,
        header: PartialDataColumnHeader,
    ) -> Result<(), String> {
        let Self::Artificial(seen) = self else {
            return Err("declared cached header requires isolated cache".into())
        };
        seen.fixture_preload_partial_header(root, header).map_err(|error| error.to_string())
    }
    fn validate_complete(
        &mut self,
        graph: &FixtureGraph,
        sidecar: &DataColumnSidecar,
        time: u64,
        subnet: u64,
        settings: &KzgSettings,
    ) -> Result<(), ColumnGossipError> {
        match (self, graph) {
            (Self::Runtime(seen), FixtureGraph::Runtime(store)) => {
                seen.validate_complete(store, sidecar, time, subnet, settings)
            }
            (Self::Artificial(seen), FixtureGraph::Artificial(store)) => {
                seen.validate_complete(store, sidecar, time, subnet, settings)
            }
            _ => Err(ColumnGossipError::Reject("fixture cache and graph policy differ")),
        }
    }
    fn validate_partial(
        &mut self,
        graph: &FixtureGraph,
        sidecar: &PartialDataColumnSidecar,
        time: u64,
        group: &PartialDataColumnGroupID,
        column: u64,
        settings: &KzgSettings,
    ) -> Result<(), ColumnGossipError> {
        match (self, graph) {
            (Self::Runtime(seen), FixtureGraph::Runtime(store)) => {
                seen.validate_partial(store, sidecar, time, group, column, settings)
            }
            (Self::Artificial(seen), FixtureGraph::Artificial(store)) => {
                seen.validate_partial(store, sidecar, time, group, column, settings)
            }
            _ => Err(ColumnGossipError::Reject("fixture cache and graph policy differ")),
        }
    }
}
