//! Published capella processing fixture adapter.

use super::{
    bellatrix, capella, capella_processing, config_override, config_version_override,
    fixture_disables_bls, hex, poll_fixture_future, read_snappy_ssz, schema_preset, yaml, Case,
    FixtureExecutionEngine,
};

pub(super) fn run_capella_processing(case: &Case, max_bytes: usize) -> Result<(), String> {
    let preset = schema_preset(case)?;
    let spec_only = fixture_disables_bls(case, max_bytes)?;
    let bytes = read_snappy_ssz(&case.path.join("pre.ssz_snappy"), max_bytes)?;
    let mut parameters = capella_processing::CapellaParameters::for_preset(preset);
    super::fixture_config::apply_phase0(case, max_bytes, &mut parameters.bellatrix.altair.phase0)?;
    parameters.fork_version =
        config_version_override(case, "CAPELLA_FORK_VERSION", parameters.fork_version, max_bytes)?;
    parameters.bellatrix.altair.inactivity_score_bias = config_override(
        case,
        "INACTIVITY_SCORE_BIAS",
        parameters.bellatrix.altair.inactivity_score_bias,
        max_bytes,
    )?;
    parameters.bellatrix.altair.inactivity_score_recovery_rate = config_override(
        case,
        "INACTIVITY_SCORE_RECOVERY_RATE",
        parameters.bellatrix.altair.inactivity_score_recovery_rate,
        max_bytes,
    )?;
    parameters.bellatrix.seconds_per_slot = config_override(
        case,
        "SECONDS_PER_SLOT",
        parameters.bellatrix.seconds_per_slot,
        max_bytes,
    )?;
    let duration = config_override(
        case,
        "SLOT_DURATION_MS",
        parameters.bellatrix.seconds_per_slot.checked_mul(1000).ok_or("slot duration overflow")?,
        max_bytes,
    )?;
    if !duration.is_multiple_of(1000) {
        return Err("Capella fixture requires an integral seconds clock".into())
    }
    parameters.bellatrix.seconds_per_slot = duration / 1000;
    parameters.bellatrix.altair.fork_version = config_version_override(
        case,
        "ALTAIR_FORK_VERSION",
        parameters.bellatrix.altair.fork_version,
        max_bytes,
    )?;
    parameters.bellatrix.fork_version = config_version_override(
        case,
        "BELLATRIX_FORK_VERSION",
        parameters.bellatrix.fork_version,
        max_bytes,
    )?;
    parameters.validate().map_err(|error| error.to_string())?;
    let mut state = if case.runner == "fork" {
        let pre =
            bellatrix::BeaconState::from_ssz(&bytes, preset).map_err(|error| error.to_string())?;
        capella_processing::upgrade_to_capella(&pre, &parameters)
            .map_err(|error| error.to_string())?
    } else {
        capella::BeaconState::from_ssz(&bytes, preset).map_err(|error| error.to_string())?
    };
    macro_rules! operation {
        ($handler:ident, $value:expr $(,)?) => {
            capella_processing::$handler(&mut state, $value, &parameters)
        };
    }
    let result: Result<(), String> = if case.runner == "operations" &&
        case.handler == "execution_payload"
    {
        let body = capella::BeaconBlockBody::from_ssz(
            &read_snappy_ssz(&case.path.join("body.ssz_snappy"), max_bytes)?,
            preset,
        )
        .map_err(|error| error.to_string())?;
        let engine = FixtureExecutionEngine::from_case(case, max_bytes, true)?;
        poll_fixture_future(capella_processing::process_execution_payload(
            &mut state,
            &body,
            &parameters,
            &engine,
        ))?
        .map_err(|error| error.to_string())
    } else if case.runner == "fork" {
        Ok(())
    } else if case.runner == "operations" {
        let operation_bytes = read_snappy_ssz(
            &case.path.join(format!(
                "{}.ssz_snappy",
                if case.handler == "block_header" {
                    "block"
                } else if case.handler == "withdrawals" {
                    "execution_payload"
                } else if case.handler == "bls_to_execution_change" {
                    "address_change"
                } else {
                    &case.handler
                }
            )),
            max_bytes,
        )?;
        match case.handler.as_str() {
            "block_header" => capella_processing::process_block_header(
                &mut state,
                &capella::BeaconBlock::from_ssz(&operation_bytes, preset)
                    .map_err(|error| error.to_string())?,
                &parameters,
            ),
            "attestation" => {
                let value = capella::Attestation::from_ssz(&operation_bytes, preset)
                    .map_err(|error| error.to_string())?;
                if spec_only {
                    capella_processing::spec_only::process_attestation(
                        &mut state,
                        &value,
                        &parameters,
                    )
                } else {
                    capella_processing::process_attestation(&mut state, &value, &parameters)
                }
            }
            "attester_slashing" => operation!(
                process_attester_slashing,
                &capella::AttesterSlashing::from_ssz(&operation_bytes, preset)
                    .map_err(|error| error.to_string())?,
            ),
            "proposer_slashing" => operation!(
                process_proposer_slashing,
                &capella::ProposerSlashing::from_ssz(&operation_bytes, preset)
                    .map_err(|error| error.to_string())?,
            ),
            "deposit" => operation!(
                process_deposit,
                &capella::Deposit::from_ssz(&operation_bytes, preset)
                    .map_err(|error| error.to_string())?,
            ),
            "voluntary_exit" => operation!(
                process_voluntary_exit,
                &capella::SignedVoluntaryExit::from_ssz(&operation_bytes, preset)
                    .map_err(|error| error.to_string())?,
            ),
            "withdrawals" => operation!(
                process_withdrawals,
                &capella::ExecutionPayload::from_ssz(&operation_bytes, preset)
                    .map_err(|error| error.to_string())?
            ),
            "bls_to_execution_change" => operation!(
                process_bls_to_execution_change,
                &capella::SignedBLSToExecutionChange::from_ssz(&operation_bytes, preset)
                    .map_err(|error| error.to_string())?
            ),
            "sync_aggregate" => operation!(
                process_sync_aggregate,
                &capella::SyncAggregate::from_ssz(&operation_bytes, preset)
                    .map_err(|error| error.to_string())?,
            ),
            _ => return Err("unsupported Capella operation handler".into()),
        }
        .map_err(|error| error.to_string())
    } else if case.runner == "sanity" && case.handler == "blocks" {
        let meta = yaml(&case.path.join("meta.yaml"), max_bytes)?;
        let count = meta["blocks_count"].as_u64().ok_or("block count must be unsigned")?;
        if count == 0 || count > 1024 {
            return Err("block count exceeds bounded fixture budget".into());
        }
        let blocks = (0..count)
            .map(|index| {
                let bytes = read_snappy_ssz(
                    &case.path.join(format!("blocks_{index}.ssz_snappy")),
                    max_bytes,
                )?;
                capella::SignedBeaconBlock::from_ssz(&bytes, preset)
                    .map_err(|error| error.to_string())
            })
            .collect::<Result<Vec<_>, String>>()?;
        let engine = FixtureExecutionEngine::from_case(case, max_bytes, false)?;
        blocks.iter().try_for_each(|block| {
            poll_fixture_future(capella_processing::process_signed_block(
                &mut state,
                block,
                &parameters,
                &engine,
            ))?
            .map_err(|error| error.to_string())
        })
    } else if case.runner == "sanity" {
        let slots = yaml(&case.path.join("slots.yaml"), max_bytes)?;
        let count = slots
            .as_u64()
            .or_else(|| slots.as_str().and_then(|value| value.parse().ok()))
            .ok_or("slot fixture count must be unsigned")?;
        if count > 8192 {
            return Err("slot fixture gap exceeds bounded replay limit".into());
        }
        let target = state.slot.checked_add(count).ok_or("slot fixture target overflow")?;
        capella_processing::process_slots(&mut state, target, &parameters)
            .map_err(|error| error.to_string())
    } else {
        match case.handler.as_str() {
            "justification_and_finalization" => {
                capella_processing::process_justification_and_finalization(&mut state, &parameters)
            }
            "rewards_and_penalties" => {
                capella_processing::process_rewards_and_penalties(&mut state, &parameters)
            }
            "registry_updates" => {
                capella_processing::process_registry_updates(&mut state, &parameters)
            }
            "slashings" => capella_processing::process_slashings(&mut state, &parameters),
            "eth1_data_reset" => {
                capella_processing::process_eth1_data_reset(&mut state, &parameters)
            }
            "effective_balance_updates" => {
                capella_processing::process_effective_balance_updates(&mut state, &parameters)
            }
            "slashings_reset" => {
                capella_processing::process_slashings_reset(&mut state, &parameters)
            }
            "randao_mixes_reset" => {
                capella_processing::process_randao_mixes_reset(&mut state, &parameters)
            }
            "historical_summaries_update" => {
                capella_processing::process_historical_summaries_update(&mut state, &parameters)
            }
            "participation_flag_updates" => {
                capella_processing::process_participation_flag_updates(&mut state, &parameters)
            }
            "inactivity_updates" => {
                capella_processing::process_inactivity_updates(&mut state, &parameters)
            }
            "sync_committee_updates" => {
                capella_processing::process_sync_committee_updates(&mut state, &parameters)
            }
            _ => return Err("unsupported Capella processing handler".into()),
        }
        .map_err(|error| error.to_string())
    };
    let post_path = case.path.join("post.ssz_snappy");
    match result {
        Err(error) if post_path.is_file() => return Err(error),
        Err(_) => return Ok(()),
        Ok(()) if !post_path.is_file() => {
            return Err("expected processing rejection, received success".into())
        }
        Ok(()) => {}
    }
    let post_bytes = read_snappy_ssz(&post_path, max_bytes)?;
    let post = reth_embedded_beacon_state_transition::ssz::capella::BeaconState::from_ssz(
        &post_bytes,
        preset,
    )
    .map_err(|error| error.to_string())?;
    let root = state.hash_tree_root(preset).map_err(|error| error.to_string())?;
    let expected_root = post.hash_tree_root(preset).map_err(|error| error.to_string())?;
    if root != expected_root {
        return Err(format!(
            "processing post-state root mismatch: expected {}, received {}",
            hex(&expected_root),
            hex(&root)
        ));
    }
    if state.to_ssz(preset).map_err(|error| error.to_string())? != post_bytes {
        return Err(
            "processing post-state differs from the original published canonical bytes".into()
        );
    }
    Ok(())
}
