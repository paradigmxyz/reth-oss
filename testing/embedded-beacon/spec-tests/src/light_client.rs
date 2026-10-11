//! Published light-client synchronization and ranked-update fixtures.
//!
//! Bootstrap roots and schedules come from authenticated fixture metadata. Every
//! update goes through the production proof and aggregate signature verifier.

use super::{
    config_override, config_version_override, fixture_gloas_parameters, read_snappy_ssz,
    schema_fork, schema_fork_name, schema_preset, yaml, Case, Outcome, FORKS,
};
use reth_embedded_beacon_primitives::{hash_pair, root_from_hex, sha256, Fork, Root};
use reth_embedded_beacon_state_transition::{
    fork_processing::{ForkActivation, TransitionParameters},
    light_client::{self, Bootstrap, Header, LightClientParameters, LightClientStore, Update},
    ssz::Preset,
};
use serde_yaml::Value;

pub(super) fn supports(case: &Case) -> bool {
    case.runner == "light_client" &&
        matches!(case.handler.as_str(), "sync" | "update_ranking" | "single_merkle_proof") &&
        FORKS[1..].contains(&case.fork.as_str())
}

pub(super) fn run_case(case: &Case, max_bytes: usize) -> Result<(), (Outcome, String)> {
    let result = match case.handler.as_str() {
        "sync" => run_sync(case, max_bytes),
        "update_ranking" => run_ranking(case, max_bytes),
        "single_merkle_proof" => run_merkle(case, max_bytes),
        _ => {
            return Err((
                Outcome::Unsupported,
                "required light-client handler is unavailable".into(),
            ))
        }
    };
    result.map_err(|error| (Outcome::Failed, error))
}

pub(super) fn parameters(
    case: &Case,
    preset: Preset,
    genesis: Root,
    max_bytes: usize,
) -> Result<LightClientParameters, String> {
    let mut kernels = fixture_gloas_parameters(case, preset, max_bytes)?;
    let current = schema_fork(case)?;
    let mut schedule = Vec::new();
    for (index, name) in FORKS.iter().enumerate() {
        let fork = schema_fork_name(name)?;
        let version_name = if fork == Fork::Phase0 {
            "GENESIS_FORK_VERSION".to_owned()
        } else {
            format!("{}_FORK_VERSION", name.to_ascii_uppercase())
        };
        let version = config_version_override(
            case,
            &version_name,
            [index as u8, 0, 0, u8::from(preset == Preset::Minimal)],
            max_bytes,
        )?;
        let epoch = if fork == Fork::Phase0 {
            0
        } else {
            config_override(
                case,
                &format!("{}_FORK_EPOCH", name.to_ascii_uppercase()),
                if fork <= current { 0 } else { u64::MAX },
                max_bytes,
            )?
        };
        schedule.push(ForkActivation { fork, epoch, version });
    }
    if schedule[6].epoch == u64::MAX {
        kernels.fulu.blob_schedule.clear();
    }
    let parameters = LightClientParameters {
        transitions: TransitionParameters::new(kernels, schedule)
            .map_err(|error| error.to_string())?,
        genesis_validators_root: genesis,
    };
    parameters.validate().map_err(|error| error.to_string())?;
    Ok(parameters)
}

pub(super) const fn data_schema(fork: Fork) -> Fork {
    match fork {
        Fork::Bellatrix => Fork::Altair,
        Fork::Fulu => Fork::Electra,
        other => other,
    }
}

pub(super) fn version_schema(
    value: &Value,
    parameters: &LightClientParameters,
) -> Result<Fork, String> {
    let bytes = super::fixed_hex::<4>(value)?;
    let mut matches = parameters
        .transitions
        .schedule
        .iter()
        .filter(|entry| entry.version == bytes)
        .map(|entry| data_schema(entry.fork));
    let schema = matches.next().ok_or("unknown light-client store fork version")?;
    if matches.next().is_some() {
        return Err("ambiguous light-client store fork version".into());
    }
    Ok(schema)
}

pub(super) fn digest(
    parameters: &LightClientParameters,
    fork: Fork,
    epoch: u64,
) -> Result<[u8; 4], String> {
    let activation = parameters
        .transitions
        .schedule
        .iter()
        .find(|entry| entry.fork == fork)
        .ok_or("missing fork version")?;
    let mut version = [0; 32];
    version[..4].copy_from_slice(&activation.version);
    let mut root = hash_pair(version, parameters.genesis_validators_root);
    if fork >= Fork::Fulu {
        let blobs = parameters.transitions.gloas.fulu.blob_parameters_at_epoch(epoch);
        let mut encoded = [0; 16];
        encoded[..8].copy_from_slice(&blobs.epoch.to_le_bytes());
        encoded[8..].copy_from_slice(&blobs.max_blobs_per_block.to_le_bytes());
        for (byte, mask) in root.iter_mut().zip(sha256(&encoded)) {
            *byte ^= mask;
        }
    }
    Ok(root[..4].try_into().map_err(|_| "invalid fork digest")?)
}

pub(super) fn digest_schema(
    value: &Value,
    parameters: &LightClientParameters,
) -> Result<Fork, String> {
    let bytes = super::fixed_hex::<4>(value)?;
    if bytes.len() != 4 {
        return Err("fork digest length differs from four".into());
    }
    let mut epochs = vec![0, parameters.transitions.gloas.fulu.electra_fork_epoch];
    epochs.extend(parameters.transitions.gloas.fulu.blob_schedule.iter().map(|entry| entry.epoch));
    let mut found = None;
    for entry in &parameters.transitions.schedule {
        if entry.fork == Fork::Phase0 {
            continue;
        }
        for epoch in &epochs {
            if digest(parameters, entry.fork, *epoch)? == bytes {
                if found.is_some_and(|old| old != entry.fork) {
                    return Err("ambiguous fixture fork digest".into());
                }
                found = Some(entry.fork);
            }
        }
    }
    found.ok_or_else(|| {
        "fixture fork digest is absent from its trusted fork and blob schedule".into()
    })
}

pub(super) fn check_digest(
    value: &Value,
    parameters: &LightClientParameters,
    fork: Fork,
    slot: u64,
) -> Result<(), String> {
    let expected = super::fixed_hex::<4>(value)?;
    if digest(parameters, fork, slot / parameters.slots_per_epoch())? != expected {
        return Err("fixture digest differs from exact header-slot blob parameters".into());
    }
    Ok(())
}

pub(super) fn named_bytes(case: &Case, name: &str, max_bytes: usize) -> Result<Vec<u8>, String> {
    if name.is_empty() || !name.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'_') {
        return Err("invalid fixture member name".into());
    }
    read_snappy_ssz(&case.path.join(format!("{name}.ssz_snappy")), max_bytes)
}

fn run_sync(case: &Case, max_bytes: usize) -> Result<(), String> {
    let meta = yaml(&case.path.join("meta.yaml"), max_bytes)?;
    let preset = schema_preset(case)?;
    let genesis = root_from_hex(
        meta["genesis_validators_root"].as_str().ok_or("missing genesis validator root")?,
    )
    .map_err(|error| error.to_string())?;
    let parameters = parameters(case, preset, genesis, max_bytes)?;
    let wire_fork = digest_schema(&meta["bootstrap_fork_digest"], &parameters)?;
    let bootstrap = Bootstrap::from_ssz(
        &named_bytes(case, "bootstrap", max_bytes)?,
        data_schema(wire_fork),
        preset,
    )
    .map_err(|error| error.to_string())?;
    check_digest(
        &meta["bootstrap_fork_digest"],
        &parameters,
        wire_fork,
        bootstrap.header.beacon().slot,
    )?;
    let target = version_schema(&meta["store_fork_version"], &parameters)?;
    let bootstrap = light_client::upgrade_bootstrap(&bootstrap, target, &parameters)
        .map_err(|error| error.to_string())?;
    let trusted =
        root_from_hex(meta["trusted_block_root"].as_str().ok_or("missing trusted bootstrap root")?)
            .map_err(|error| error.to_string())?;
    let mut store = light_client::initialize_light_client_store(trusted, &bootstrap, &parameters)
        .map_err(|error| error.to_string())?;
    let steps = yaml(&case.path.join("steps.yaml"), max_bytes)?;
    let steps = steps.as_sequence().ok_or("light-client steps must be a list")?;
    if steps.is_empty() || steps.len() > 100_000 {
        return Err("light-client step count exceeds its resource bound".into());
    }
    for (index, step) in steps.iter().enumerate() {
        let event = step.as_mapping().ok_or("light-client event must be a mapping")?;
        if event.len() != 1 {
            return Err("light-client event must contain exactly one action".into());
        }
        let (kind, action) = event.iter().next().ok_or("empty event")?;
        let result = match kind.as_str() {
            Some("process_update") => {
                let fork = digest_schema(&action["update_fork_digest"], &parameters)?;
                let name = action["update"].as_str().ok_or("missing update fixture name")?;
                let update = Update::from_ssz(
                    &named_bytes(case, name, max_bytes)?,
                    data_schema(fork),
                    preset,
                )
                .map_err(|error| error.to_string())?;
                check_digest(
                    &action["update_fork_digest"],
                    &parameters,
                    fork,
                    update.attested_header.beacon().slot,
                )?;
                let update = light_client::upgrade_update(&update, store.schema(), &parameters)
                    .map_err(|error| error.to_string())?;
                light_client::process_light_client_update(
                    &mut store,
                    &update,
                    action["current_slot"].as_u64().ok_or("missing current slot")?,
                    &parameters,
                )
            }
            Some("force_update") => light_client::process_light_client_store_force_update(
                &mut store,
                action["current_slot"].as_u64().ok_or("missing force-update slot")?,
                &parameters,
            ),
            Some("upgrade_store") => {
                let target = version_schema(&action["store_fork_version"], &parameters)?;
                light_client::upgrade_store(&mut store, target, &parameters)
            }
            _ => return Err("required light-client event is not implemented".into()),
        };
        result.map_err(|error| format!("light-client step {index}: {error}"))?;
        checks(&action["checks"], &store, &parameters)
            .map_err(|error| format!("light-client step {index}: {error}"))?;
    }
    Ok(())
}

fn checks(
    value: &Value,
    store: &LightClientStore,
    parameters: &LightClientParameters,
) -> Result<(), String> {
    let mapping = value.as_mapping().ok_or("missing light-client checks")?;
    if mapping.len() != 2 ||
        mapping
            .keys()
            .any(|key| !matches!(key.as_str(), Some("finalized_header" | "optimistic_header")))
    {
        return Err("required light-client check is not implemented".into());
    }
    for (name, actual) in [
        ("finalized_header", store.finalized_header()),
        ("optimistic_header", store.optimistic_header()),
    ] {
        header_checks(&value[name], actual, parameters)?;
    }
    Ok(())
}

fn header_checks(
    value: &Value,
    header: &Header,
    parameters: &LightClientParameters,
) -> Result<(), String> {
    let mapping = value.as_mapping().ok_or("missing header checks")?;
    if mapping
        .keys()
        .any(|key| !matches!(key.as_str(), Some("slot" | "beacon_root" | "execution_root")))
    {
        return Err("required header check is not implemented".into());
    }
    if value["slot"].as_u64() != Some(header.beacon().slot) {
        return Err("header slot differs from published check".into());
    }
    let expected = root_from_hex(value["beacon_root"].as_str().ok_or("missing beacon root check")?)
        .map_err(|error| error.to_string())?;
    if header.beacon().hash_tree_root() != expected {
        return Err("beacon header root differs from published check".into());
    }
    if let Some(expected) = value["execution_root"].as_str() {
        let expected = root_from_hex(expected).map_err(|error| error.to_string())?;
        let actual = light_client::get_lc_execution_root(header, parameters)
            .map_err(|error| error.to_string())?;
        if actual != expected {
            return Err("execution root differs from published check".into());
        }
    }
    Ok(())
}

fn run_ranking(case: &Case, max_bytes: usize) -> Result<(), String> {
    let meta = yaml(&case.path.join("meta.yaml"), max_bytes)?;
    let count = meta["updates_count"].as_u64().ok_or("missing ranked-update count")?;
    if !(2..=1024).contains(&count) {
        return Err("ranked-update count exceeds resource bound".into());
    }
    let preset = schema_preset(case)?;
    let fork = data_schema(schema_fork(case)?);
    let parameters = parameters(case, preset, [0; 32], max_bytes)?;
    let mut previous = Update::from_ssz(&named_bytes(case, "updates_0", max_bytes)?, fork, preset)
        .map_err(|error| error.to_string())?;
    for index in 1..count {
        let next = Update::from_ssz(
            &named_bytes(case, &format!("updates_{index}"), max_bytes)?,
            fork,
            preset,
        )
        .map_err(|error| error.to_string())?;
        if !light_client::is_better_update(&previous, &next, &parameters)
            .map_err(|error| error.to_string())?
        {
            return Err(format!(
                "published update ranking is incorrect at pair {} / {index}",
                index - 1
            ));
        }
        previous = next;
    }
    Ok(())
}

pub(super) fn run_merkle(case: &Case, max_bytes: usize) -> Result<(), String> {
    let preset = schema_preset(case)?;
    let fork = schema_fork(case)?;
    let bytes = named_bytes(case, "object", max_bytes)?;
    let proof = yaml(&case.path.join("proof.yaml"), max_bytes)?;
    let leaf = super::fixed_hex::<32>(&proof["leaf"])?;
    let gindex = proof["leaf_index"].as_u64().ok_or("missing proof generalized index")?;
    if gindex == 0 {
        return Err("zero generalized index is invalid".into());
    }
    let branch = proof["branch"]
        .as_sequence()
        .ok_or("missing proof branch")?
        .iter()
        .map(super::fixed_hex::<32>)
        .collect::<Result<Vec<_>, _>>()?;
    if branch.len() > 64 {
        return Err("proof branch exceeds generalized index bound".into());
    }
    let (actual_leaf, actual_branch, actual_root) = if case.suite == "BeaconState" {
        let state =
            reth_embedded_beacon_state_transition::ssz::BeaconState::from_ssz(&bytes, fork, preset)
                .map_err(|error| error.to_string())?;
        (
            light_client::merkle::state_tree_node(&state, gindex)
                .map_err(|error| error.to_string())?,
            light_client::merkle::compute_state_merkle_proof(&state, gindex)
                .map_err(|error| error.to_string())?,
            state.hash_tree_root().map_err(|error| error.to_string())?,
        )
    } else if case.suite == "BeaconBlockBody" {
        macro_rules! body {
            ($module:ident) => {{
                let body =
                    reth_embedded_beacon_state_transition::ssz::$module::BeaconBlockBody::from_ssz(
                        &bytes, preset,
                    )
                    .map_err(|error| error.to_string())?;
                if body.to_ssz(preset).map_err(|error| error.to_string())? != bytes {
                    return Err("Merkle proof object is not canonical SSZ".into());
                }
                (
                    light_client::merkle::tree_node(&body, gindex, preset)
                        .map_err(|error| error.to_string())?,
                    light_client::merkle::compute_merkle_proof(&body, gindex, preset)
                        .map_err(|error| error.to_string())?,
                    body.hash_tree_root(preset).map_err(|error| error.to_string())?,
                )
            }};
        }
        match fork {
            Fork::Altair => body!(altair),
            Fork::Bellatrix => body!(bellatrix),
            Fork::Capella => body!(capella),
            Fork::Deneb => body!(deneb),
            Fork::Electra => body!(electra),
            Fork::Fulu => body!(fulu),
            Fork::Gloas => body!(gloas),
            Fork::Phase0 => return Err("Phase0 light-client proof is unsupported".into()),
        }
    } else {
        return Err("required Merkle-proof object is not implemented".into());
    };
    if actual_leaf != leaf || actual_branch != branch {
        return Err("generated proof differs from the exact published leaf or branch".into());
    }
    let mut node = leaf;
    let mut index = gindex;
    for sibling in branch {
        node = if index.is_multiple_of(2) {
            hash_pair(node, sibling)
        } else {
            hash_pair(sibling, node)
        };
        index /= 2;
    }
    if index != 1 || node != actual_root {
        return Err("published proof does not reconstruct the complete typed object root".into());
    }
    Ok(())
}
