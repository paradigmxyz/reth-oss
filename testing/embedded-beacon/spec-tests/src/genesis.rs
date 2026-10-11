//! Authenticated genesis cases call strict production initialization and validity helpers.

use super::{
    config_override, config_version_override, fixed_hex, fixture_disables_bls, read_snappy_ssz,
    schema_preset, yaml, Case, Outcome,
};
use reth_embedded_beacon_state_transition::{
    phase0_processing::genesis::{
        initialize_beacon_state_from_eth1, is_valid_genesis_state, GenesisParameters,
    },
    ssz::phase0,
};

pub(super) fn supports(case: &Case) -> bool {
    case.fork == "phase0" &&
        case.runner == "genesis" &&
        matches!(case.handler.as_str(), "initialization" | "validity") &&
        matches!(case.preset.as_str(), "mainnet" | "minimal")
}

pub(super) fn run_case(case: &Case, max_bytes: usize) -> Result<(), (Outcome, String)> {
    run_genesis(case, max_bytes).map_err(|error| (Outcome::Failed, error))
}

fn run_genesis(case: &Case, max_bytes: usize) -> Result<(), String> {
    let preset = schema_preset(case)?;
    let mut parameters = GenesisParameters::for_preset(preset);
    parameters.genesis_delay =
        config_override(case, "GENESIS_DELAY", parameters.genesis_delay, max_bytes)?;
    parameters.min_genesis_time =
        config_override(case, "MIN_GENESIS_TIME", parameters.min_genesis_time, max_bytes)?;
    parameters.min_genesis_active_validator_count = config_override(
        case,
        "MIN_GENESIS_ACTIVE_VALIDATOR_COUNT",
        parameters.min_genesis_active_validator_count,
        max_bytes,
    )?;
    parameters.phase0.genesis_fork_version = config_version_override(
        case,
        "GENESIS_FORK_VERSION",
        parameters.phase0.genesis_fork_version,
        max_bytes,
    )?;
    parameters.validate().map_err(|error| error.to_string())?;
    if fixture_disables_bls(case, max_bytes)? {
        return Err("strict genesis fixtures cannot disable deposit signature verification".into())
    }
    if case.handler == "initialization" {
        let eth1 = yaml(&case.path.join("eth1.yaml"), max_bytes)?;
        let block_hash = fixed_hex::<32>(&eth1["eth1_block_hash"])?;
        let timestamp = eth1["eth1_timestamp"].as_u64().ok_or("missing execution timestamp")?;
        let metadata = yaml(&case.path.join("meta.yaml"), max_bytes)?;
        let deposit_count = metadata["deposits_count"].as_u64().ok_or("missing deposit count")?;
        let deposit_count = usize::try_from(deposit_count)
            .map_err(|_| "genesis deposit count does not fit platform")?;
        let deposit_bytes =
            deposit_count.checked_mul(1_240).ok_or("genesis deposit budget overflow")?;
        if deposit_bytes > max_bytes {
            return Err("genesis deposit input exceeds aggregate case byte budget".into())
        }
        let mut deposits = Vec::new();
        deposits
            .try_reserve_exact(deposit_count)
            .map_err(|_| "unable to allocate bounded genesis deposits")?;
        for index in 0..deposit_count {
            let bytes = read_snappy_ssz(
                &case.path.join(format!("deposits_{index}.ssz_snappy")),
                max_bytes,
            )?;
            deposits.push(
                phase0::Deposit::from_ssz(&bytes, preset).map_err(|error| error.to_string())?,
            );
        }
        let state =
            initialize_beacon_state_from_eth1(block_hash, timestamp, &deposits, &parameters)
                .map_err(|error| error.to_string())?;
        let expected = read_snappy_ssz(&case.path.join("state.ssz_snappy"), max_bytes)?;
        phase0::BeaconState::from_ssz(&expected, preset).map_err(|error| error.to_string())?;
        let actual = state.to_ssz(preset).map_err(|error| error.to_string())?;
        if actual != expected {
            return Err("initialized genesis state differs from exact published SSZ".into())
        }
    } else {
        let bytes = read_snappy_ssz(&case.path.join("genesis.ssz_snappy"), max_bytes)?;
        let state =
            phase0::BeaconState::from_ssz(&bytes, preset).map_err(|error| error.to_string())?;
        let expected = yaml(&case.path.join("is_valid.yaml"), max_bytes)?
            .as_bool()
            .ok_or("missing genesis validity boolean")?;
        let actual =
            is_valid_genesis_state(&state, &parameters).map_err(|error| error.to_string())?;
        if actual != expected {
            return Err(format!("genesis validity mismatch: expected {expected}, received {actual}"))
        }
    }
    Ok(())
}
