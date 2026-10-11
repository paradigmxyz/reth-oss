//! Published electra processing fixture adapter.

use super::{
    config_override, config_version_override, deneb, electra, electra_processing,
    fixture_disables_bls, hex, poll_fixture_future, read_snappy_ssz, schema_preset, yaml, Case,
    FixtureExecutionEngine,
};

pub(super) fn run_electra_processing(case: &Case, max_bytes: usize) -> Result<(), String> {
    let preset = schema_preset(case)?;
    let spec_only = fixture_disables_bls(case, max_bytes)?;
    let bytes = read_snappy_ssz(&case.path.join("pre.ssz_snappy"), max_bytes)?;
    let mut parameters = electra_processing::ElectraParameters::for_preset(preset);
    parameters.deneb.capella.bellatrix.altair.phase0.ejection_balance = config_override(
        case,
        "EJECTION_BALANCE",
        parameters.deneb.capella.bellatrix.altair.phase0.ejection_balance,
        max_bytes,
    )?;
    parameters.deneb.capella.bellatrix.altair.phase0.min_per_epoch_churn_limit = config_override(
        case,
        "MIN_PER_EPOCH_CHURN_LIMIT",
        parameters.deneb.capella.bellatrix.altair.phase0.min_per_epoch_churn_limit,
        max_bytes,
    )?;
    parameters.deneb.capella.bellatrix.altair.phase0.churn_limit_quotient = config_override(
        case,
        "CHURN_LIMIT_QUOTIENT",
        parameters.deneb.capella.bellatrix.altair.phase0.churn_limit_quotient,
        max_bytes,
    )?;
    parameters.deneb.capella.bellatrix.altair.phase0.min_validator_withdrawability_delay =
        config_override(
            case,
            "MIN_VALIDATOR_WITHDRAWABILITY_DELAY",
            parameters.deneb.capella.bellatrix.altair.phase0.min_validator_withdrawability_delay,
            max_bytes,
        )?;
    parameters.deneb.capella.bellatrix.altair.phase0.shard_committee_period = config_override(
        case,
        "SHARD_COMMITTEE_PERIOD",
        parameters.deneb.capella.bellatrix.altair.phase0.shard_committee_period,
        max_bytes,
    )?;
    parameters.deneb.capella.bellatrix.altair.phase0.genesis_fork_version =
        config_version_override(
            case,
            "GENESIS_FORK_VERSION",
            parameters.deneb.capella.bellatrix.altair.phase0.genesis_fork_version,
            max_bytes,
        )?;
    parameters.fork_version =
        config_version_override(case, "ELECTRA_FORK_VERSION", parameters.fork_version, max_bytes)?;
    parameters.deneb.capella.bellatrix.altair.inactivity_score_bias = config_override(
        case,
        "INACTIVITY_SCORE_BIAS",
        parameters.deneb.capella.bellatrix.altair.inactivity_score_bias,
        max_bytes,
    )?;
    parameters.deneb.capella.bellatrix.altair.inactivity_score_recovery_rate = config_override(
        case,
        "INACTIVITY_SCORE_RECOVERY_RATE",
        parameters.deneb.capella.bellatrix.altair.inactivity_score_recovery_rate,
        max_bytes,
    )?;
    parameters.deneb.capella.bellatrix.seconds_per_slot = config_override(
        case,
        "SECONDS_PER_SLOT",
        parameters.deneb.capella.bellatrix.seconds_per_slot,
        max_bytes,
    )?;
    let duration = config_override(
        case,
        "SLOT_DURATION_MS",
        parameters
            .deneb
            .capella
            .bellatrix
            .seconds_per_slot
            .checked_mul(1000)
            .ok_or("slot duration overflow")?,
        max_bytes,
    )?;
    if !duration.is_multiple_of(1000) {
        return Err("Electra fixture requires an integral seconds clock".into())
    }
    parameters.deneb.capella.bellatrix.seconds_per_slot = duration / 1000;
    parameters.deneb.capella.bellatrix.altair.fork_version = config_version_override(
        case,
        "ALTAIR_FORK_VERSION",
        parameters.deneb.capella.bellatrix.altair.fork_version,
        max_bytes,
    )?;
    parameters.deneb.capella.bellatrix.fork_version = config_version_override(
        case,
        "BELLATRIX_FORK_VERSION",
        parameters.deneb.capella.bellatrix.fork_version,
        max_bytes,
    )?;
    parameters.deneb.capella.fork_version = config_version_override(
        case,
        "CAPELLA_FORK_VERSION",
        parameters.deneb.capella.fork_version,
        max_bytes,
    )?;
    parameters.max_blobs_per_block = config_override(
        case,
        "MAX_BLOBS_PER_BLOCK_ELECTRA",
        parameters.max_blobs_per_block,
        max_bytes,
    )?;
    parameters.deneb.max_per_epoch_activation_churn_limit = config_override(
        case,
        "MAX_PER_EPOCH_ACTIVATION_CHURN_LIMIT",
        parameters.deneb.max_per_epoch_activation_churn_limit,
        max_bytes,
    )?;
    parameters.deneb.fork_version = config_version_override(
        case,
        "DENEB_FORK_VERSION",
        parameters.deneb.fork_version,
        max_bytes,
    )?;
    parameters.min_per_epoch_churn_limit = config_override(
        case,
        "MIN_PER_EPOCH_CHURN_LIMIT_ELECTRA",
        parameters.min_per_epoch_churn_limit,
        max_bytes,
    )?;
    parameters.max_per_epoch_activation_exit_churn_limit = config_override(
        case,
        "MAX_PER_EPOCH_ACTIVATION_EXIT_CHURN_LIMIT",
        parameters.max_per_epoch_activation_exit_churn_limit,
        max_bytes,
    )?;
    parameters.validate().map_err(|error| error.to_string())?;
    let mut state = if case.runner == "fork" {
        let pre =
            deneb::BeaconState::from_ssz(&bytes, preset).map_err(|error| error.to_string())?;
        electra_processing::upgrade_to_electra(&pre, &parameters)
            .map_err(|error| error.to_string())?
    } else {
        electra::BeaconState::from_ssz(&bytes, preset).map_err(|error| error.to_string())?
    };
    macro_rules! operation {
        ($handler:ident, $value:expr $(,)?) => {
            electra_processing::$handler(&mut state, $value, &parameters)
        };
    }
    let result: Result<(), String> = if case.runner == "operations" &&
        case.handler == "execution_payload"
    {
        let body = electra::BeaconBlockBody::from_ssz(
            &read_snappy_ssz(&case.path.join("body.ssz_snappy"), max_bytes)?,
            preset,
        )
        .map_err(|error| error.to_string())?;
        let engine = FixtureExecutionEngine::from_case(case, max_bytes, true)?;
        poll_fixture_future(electra_processing::process_execution_payload(
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
            "block_header" => electra_processing::process_block_header(
                &mut state,
                &electra::BeaconBlock::from_ssz(&operation_bytes, preset)
                    .map_err(|error| error.to_string())?,
                &parameters,
            ),
            "attestation" => {
                let value = electra::Attestation::from_ssz(&operation_bytes, preset)
                    .map_err(|error| error.to_string())?;
                if spec_only {
                    electra_processing::spec_only::process_attestation(
                        &mut state,
                        &value,
                        &parameters,
                    )
                } else {
                    electra_processing::process_attestation(&mut state, &value, &parameters)
                }
            }
            "attester_slashing" => operation!(
                process_attester_slashing,
                &electra::AttesterSlashing::from_ssz(&operation_bytes, preset)
                    .map_err(|error| error.to_string())?,
            ),
            "proposer_slashing" => operation!(
                process_proposer_slashing,
                &electra::ProposerSlashing::from_ssz(&operation_bytes, preset)
                    .map_err(|error| error.to_string())?,
            ),
            "deposit" => operation!(
                process_deposit,
                &electra::Deposit::from_ssz(&operation_bytes, preset)
                    .map_err(|error| error.to_string())?,
            ),
            "voluntary_exit" => operation!(
                process_voluntary_exit,
                &electra::SignedVoluntaryExit::from_ssz(&operation_bytes, preset)
                    .map_err(|error| error.to_string())?,
            ),
            "deposit_request" => operation!(
                process_deposit_request,
                &electra::DepositRequest::from_ssz(&operation_bytes, preset)
                    .map_err(|error| error.to_string())?
            ),
            "withdrawal_request" => operation!(
                process_withdrawal_request,
                &electra::WithdrawalRequest::from_ssz(&operation_bytes, preset)
                    .map_err(|error| error.to_string())?
            ),
            "consolidation_request" => operation!(
                process_consolidation_request,
                &electra::ConsolidationRequest::from_ssz(&operation_bytes, preset)
                    .map_err(|error| error.to_string())?
            ),
            "withdrawals" => operation!(
                process_withdrawals,
                &electra::ExecutionPayload::from_ssz(&operation_bytes, preset)
                    .map_err(|error| error.to_string())?
            ),
            "bls_to_execution_change" => operation!(
                process_bls_to_execution_change,
                &electra::SignedBLSToExecutionChange::from_ssz(&operation_bytes, preset)
                    .map_err(|error| error.to_string())?
            ),
            "sync_aggregate" => operation!(
                process_sync_aggregate,
                &electra::SyncAggregate::from_ssz(&operation_bytes, preset)
                    .map_err(|error| error.to_string())?,
            ),
            _ => return Err("unsupported Electra operation handler".into()),
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
                electra::SignedBeaconBlock::from_ssz(&bytes, preset)
                    .map_err(|error| error.to_string())
            })
            .collect::<Result<Vec<_>, String>>()?;
        let engine = FixtureExecutionEngine::from_case(case, max_bytes, false)?;
        blocks.iter().try_for_each(|block| {
            poll_fixture_future(electra_processing::process_signed_block(
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
        electra_processing::process_slots(&mut state, target, &parameters)
            .map_err(|error| error.to_string())
    } else {
        match case.handler.as_str() {
            "justification_and_finalization" => {
                electra_processing::process_justification_and_finalization(&mut state, &parameters)
            }
            "rewards_and_penalties" => {
                electra_processing::process_rewards_and_penalties(&mut state, &parameters)
            }
            "registry_updates" => {
                electra_processing::process_registry_updates(&mut state, &parameters)
            }
            "slashings" => electra_processing::process_slashings(&mut state, &parameters),
            "eth1_data_reset" => {
                electra_processing::process_eth1_data_reset(&mut state, &parameters)
            }
            "effective_balance_updates" => {
                electra_processing::process_effective_balance_updates(&mut state, &parameters)
            }
            "slashings_reset" => {
                electra_processing::process_slashings_reset(&mut state, &parameters)
            }
            "randao_mixes_reset" => {
                electra_processing::process_randao_mixes_reset(&mut state, &parameters)
            }
            "historical_summaries_update" => {
                electra_processing::process_historical_summaries_update(&mut state, &parameters)
            }
            "participation_flag_updates" => {
                electra_processing::process_participation_flag_updates(&mut state, &parameters)
            }
            "inactivity_updates" => {
                electra_processing::process_inactivity_updates(&mut state, &parameters)
            }
            "pending_deposits" | "pending_deposits_churn" => {
                electra_processing::process_pending_deposits(&mut state, &parameters)
            }
            "pending_consolidations" => {
                electra_processing::process_pending_consolidations(&mut state, &parameters)
            }
            "sync_committee_updates" => {
                electra_processing::process_sync_committee_updates(&mut state, &parameters)
            }
            _ => return Err("unsupported Electra processing handler".into()),
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
    let post = reth_embedded_beacon_state_transition::ssz::electra::BeaconState::from_ssz(
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
