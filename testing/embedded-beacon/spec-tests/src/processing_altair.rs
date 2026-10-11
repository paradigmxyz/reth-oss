//! Published altair processing fixture adapter.

use super::{
    altair, altair_processing, config_override, config_version_override, fixture_disables_bls, hex,
    phase0, read_snappy_ssz, schema_preset, yaml, Case,
};

pub(super) fn run_altair_processing(case: &Case, max_bytes: usize) -> Result<(), String> {
    let preset = schema_preset(case)?;
    let spec_only = fixture_disables_bls(case, max_bytes)?;
    let bytes = read_snappy_ssz(&case.path.join("pre.ssz_snappy"), max_bytes)?;
    let mut parameters = altair_processing::AltairParameters::for_preset(preset);
    parameters.phase0.ejection_balance =
        config_override(case, "EJECTION_BALANCE", parameters.phase0.ejection_balance, max_bytes)?;
    parameters.phase0.min_per_epoch_churn_limit = config_override(
        case,
        "MIN_PER_EPOCH_CHURN_LIMIT",
        parameters.phase0.min_per_epoch_churn_limit,
        max_bytes,
    )?;
    parameters.phase0.churn_limit_quotient = config_override(
        case,
        "CHURN_LIMIT_QUOTIENT",
        parameters.phase0.churn_limit_quotient,
        max_bytes,
    )?;
    parameters.phase0.min_validator_withdrawability_delay = config_override(
        case,
        "MIN_VALIDATOR_WITHDRAWABILITY_DELAY",
        parameters.phase0.min_validator_withdrawability_delay,
        max_bytes,
    )?;
    parameters.phase0.shard_committee_period = config_override(
        case,
        "SHARD_COMMITTEE_PERIOD",
        parameters.phase0.shard_committee_period,
        max_bytes,
    )?;
    parameters.phase0.genesis_fork_version = config_version_override(
        case,
        "GENESIS_FORK_VERSION",
        parameters.phase0.genesis_fork_version,
        max_bytes,
    )?;
    parameters.fork_version =
        config_version_override(case, "ALTAIR_FORK_VERSION", parameters.fork_version, max_bytes)?;
    parameters.inactivity_score_bias = config_override(
        case,
        "INACTIVITY_SCORE_BIAS",
        parameters.inactivity_score_bias,
        max_bytes,
    )?;
    parameters.inactivity_score_recovery_rate = config_override(
        case,
        "INACTIVITY_SCORE_RECOVERY_RATE",
        parameters.inactivity_score_recovery_rate,
        max_bytes,
    )?;
    parameters.validate().map_err(|error| error.to_string())?;
    let mut state = if case.runner == "fork" {
        let pre =
            phase0::BeaconState::from_ssz(&bytes, preset).map_err(|error| error.to_string())?;
        altair_processing::upgrade_to_altair(&pre, &parameters)
            .map_err(|error| error.to_string())?
    } else {
        altair::BeaconState::from_ssz(&bytes, preset).map_err(|error| error.to_string())?
    };
    macro_rules! operation {
        ($handler:ident, $value:expr $(,)?) => {
            if spec_only {
                altair_processing::spec_only::$handler(&mut state, $value, &parameters)
            } else {
                altair_processing::$handler(&mut state, $value, &parameters)
            }
        };
    }
    let result = if case.runner == "fork" {
        Ok(())
    } else if case.runner == "operations" {
        let operation_bytes = read_snappy_ssz(
            &case.path.join(format!(
                "{}.ssz_snappy",
                if case.handler == "block_header" { "block" } else { &case.handler }
            )),
            max_bytes,
        )?;
        match case.handler.as_str() {
            "block_header" => altair_processing::process_block_header(
                &mut state,
                &altair::BeaconBlock::from_ssz(&operation_bytes, preset)
                    .map_err(|error| error.to_string())?,
                &parameters,
            ),
            "attestation" => operation!(
                process_attestation,
                &altair::Attestation::from_ssz(&operation_bytes, preset)
                    .map_err(|error| error.to_string())?,
            ),
            "attester_slashing" => operation!(
                process_attester_slashing,
                &altair::AttesterSlashing::from_ssz(&operation_bytes, preset)
                    .map_err(|error| error.to_string())?,
            ),
            "proposer_slashing" => operation!(
                process_proposer_slashing,
                &altair::ProposerSlashing::from_ssz(&operation_bytes, preset)
                    .map_err(|error| error.to_string())?,
            ),
            "deposit" => operation!(
                process_deposit,
                &altair::Deposit::from_ssz(&operation_bytes, preset)
                    .map_err(|error| error.to_string())?,
            ),
            "voluntary_exit" => operation!(
                process_voluntary_exit,
                &altair::SignedVoluntaryExit::from_ssz(&operation_bytes, preset)
                    .map_err(|error| error.to_string())?,
            ),
            "sync_aggregate" => operation!(
                process_sync_aggregate,
                &altair::SyncAggregate::from_ssz(&operation_bytes, preset)
                    .map_err(|error| error.to_string())?,
            ),
            _ => return Err("unsupported Altair operation handler".into()),
        }
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
                altair::SignedBeaconBlock::from_ssz(&bytes, preset)
                    .map_err(|error| error.to_string())
            })
            .collect::<Result<Vec<_>, String>>()?;
        blocks.iter().try_for_each(|block| {
            if spec_only {
                altair_processing::spec_only::state_transition(&mut state, block, &parameters)
            } else {
                altair_processing::process_signed_block(&mut state, block, &parameters)
            }
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
        altair_processing::process_slots(&mut state, target, &parameters)
    } else {
        match case.handler.as_str() {
            "justification_and_finalization" => {
                altair_processing::process_justification_and_finalization(&mut state, &parameters)
            }
            "rewards_and_penalties" => {
                altair_processing::process_rewards_and_penalties(&mut state, &parameters)
            }
            "registry_updates" => {
                altair_processing::process_registry_updates(&mut state, &parameters)
            }
            "slashings" => altair_processing::process_slashings(&mut state, &parameters),
            "eth1_data_reset" => {
                altair_processing::process_eth1_data_reset(&mut state, &parameters)
            }
            "effective_balance_updates" => {
                altair_processing::process_effective_balance_updates(&mut state, &parameters)
            }
            "slashings_reset" => {
                altair_processing::process_slashings_reset(&mut state, &parameters)
            }
            "randao_mixes_reset" => {
                altair_processing::process_randao_mixes_reset(&mut state, &parameters)
            }
            "historical_roots_update" => {
                altair_processing::process_historical_roots_update(&mut state, &parameters)
            }
            "participation_flag_updates" => {
                altair_processing::process_participation_flag_updates(&mut state, &parameters)
            }
            "inactivity_updates" => {
                altair_processing::process_inactivity_updates(&mut state, &parameters)
            }
            "sync_committee_updates" => {
                altair_processing::process_sync_committee_updates(&mut state, &parameters)
            }
            _ => return Err("unsupported Altair processing handler".into()),
        }
    };
    let post_path = case.path.join("post.ssz_snappy");
    match result {
        Err(error) if post_path.is_file() => return Err(error.to_string()),
        Err(_) => return Ok(()),
        Ok(()) if !post_path.is_file() => {
            return Err("expected processing rejection, received success".into())
        }
        Ok(()) => {}
    }
    let post_bytes = read_snappy_ssz(&post_path, max_bytes)?;
    let post = reth_embedded_beacon_state_transition::ssz::altair::BeaconState::from_ssz(
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
