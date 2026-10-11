//! Published fulu processing fixture adapter.

use super::{
    config_override, config_value_override, config_version_override, electra, fixture_disables_bls,
    fulu, fulu_processing, hex, poll_fixture_future, read_snappy_ssz, schema_preset, yaml, Case,
    FixtureExecutionEngine,
};

pub(super) fn run_fulu_processing(case: &Case, max_bytes: usize) -> Result<(), String> {
    let preset = schema_preset(case)?;
    let spec_only = fixture_disables_bls(case, max_bytes)?;
    let bytes = read_snappy_ssz(&case.path.join("pre.ssz_snappy"), max_bytes)?;
    let mut parameters = fulu_processing::FuluParameters::for_preset(preset);
    parameters.electra.deneb.capella.bellatrix.altair.phase0.ejection_balance = config_override(
        case,
        "EJECTION_BALANCE",
        parameters.electra.deneb.capella.bellatrix.altair.phase0.ejection_balance,
        max_bytes,
    )?;
    parameters.electra.deneb.capella.bellatrix.altair.phase0.min_per_epoch_churn_limit =
        config_override(
            case,
            "MIN_PER_EPOCH_CHURN_LIMIT",
            parameters.electra.deneb.capella.bellatrix.altair.phase0.min_per_epoch_churn_limit,
            max_bytes,
        )?;
    parameters.electra.deneb.capella.bellatrix.altair.phase0.churn_limit_quotient =
        config_override(
            case,
            "CHURN_LIMIT_QUOTIENT",
            parameters.electra.deneb.capella.bellatrix.altair.phase0.churn_limit_quotient,
            max_bytes,
        )?;
    parameters.electra.deneb.capella.bellatrix.altair.phase0.min_validator_withdrawability_delay =
        config_override(
            case,
            "MIN_VALIDATOR_WITHDRAWABILITY_DELAY",
            parameters
                .electra
                .deneb
                .capella
                .bellatrix
                .altair
                .phase0
                .min_validator_withdrawability_delay,
            max_bytes,
        )?;
    parameters.electra.deneb.capella.bellatrix.altair.phase0.shard_committee_period =
        config_override(
            case,
            "SHARD_COMMITTEE_PERIOD",
            parameters.electra.deneb.capella.bellatrix.altair.phase0.shard_committee_period,
            max_bytes,
        )?;
    parameters.electra.deneb.capella.bellatrix.altair.phase0.genesis_fork_version =
        config_version_override(
            case,
            "GENESIS_FORK_VERSION",
            parameters.electra.deneb.capella.bellatrix.altair.phase0.genesis_fork_version,
            max_bytes,
        )?;
    parameters.fork_version =
        config_version_override(case, "FULU_FORK_VERSION", parameters.fork_version, max_bytes)?;
    parameters.electra.deneb.capella.bellatrix.altair.inactivity_score_bias = config_override(
        case,
        "INACTIVITY_SCORE_BIAS",
        parameters.electra.deneb.capella.bellatrix.altair.inactivity_score_bias,
        max_bytes,
    )?;
    parameters.electra.deneb.capella.bellatrix.altair.inactivity_score_recovery_rate =
        config_override(
            case,
            "INACTIVITY_SCORE_RECOVERY_RATE",
            parameters.electra.deneb.capella.bellatrix.altair.inactivity_score_recovery_rate,
            max_bytes,
        )?;
    parameters.electra.deneb.capella.bellatrix.seconds_per_slot = config_override(
        case,
        "SECONDS_PER_SLOT",
        parameters.electra.deneb.capella.bellatrix.seconds_per_slot,
        max_bytes,
    )?;
    let duration = config_override(
        case,
        "SLOT_DURATION_MS",
        parameters
            .electra
            .deneb
            .capella
            .bellatrix
            .seconds_per_slot
            .checked_mul(1000)
            .ok_or("slot duration overflow")?,
        max_bytes,
    )?;
    if !duration.is_multiple_of(1000) {
        return Err("Fulu fixture requires an integral seconds clock".into())
    }
    parameters.electra.deneb.capella.bellatrix.seconds_per_slot = duration / 1000;
    parameters.electra.deneb.capella.bellatrix.altair.fork_version = config_version_override(
        case,
        "ALTAIR_FORK_VERSION",
        parameters.electra.deneb.capella.bellatrix.altair.fork_version,
        max_bytes,
    )?;
    parameters.electra.deneb.capella.bellatrix.fork_version = config_version_override(
        case,
        "BELLATRIX_FORK_VERSION",
        parameters.electra.deneb.capella.bellatrix.fork_version,
        max_bytes,
    )?;
    parameters.electra.deneb.capella.fork_version = config_version_override(
        case,
        "CAPELLA_FORK_VERSION",
        parameters.electra.deneb.capella.fork_version,
        max_bytes,
    )?;
    parameters.electra.max_blobs_per_block = config_override(
        case,
        "MAX_BLOBS_PER_BLOCK_ELECTRA",
        parameters.electra.max_blobs_per_block,
        max_bytes,
    )?;
    parameters.electra.deneb.max_per_epoch_activation_churn_limit = config_override(
        case,
        "MAX_PER_EPOCH_ACTIVATION_CHURN_LIMIT",
        parameters.electra.deneb.max_per_epoch_activation_churn_limit,
        max_bytes,
    )?;
    parameters.electra.deneb.fork_version = config_version_override(
        case,
        "DENEB_FORK_VERSION",
        parameters.electra.deneb.fork_version,
        max_bytes,
    )?;
    parameters.electra.min_per_epoch_churn_limit = config_override(
        case,
        "MIN_PER_EPOCH_CHURN_LIMIT_ELECTRA",
        parameters.electra.min_per_epoch_churn_limit,
        max_bytes,
    )?;
    parameters.electra.max_per_epoch_activation_exit_churn_limit = config_override(
        case,
        "MAX_PER_EPOCH_ACTIVATION_EXIT_CHURN_LIMIT",
        parameters.electra.max_per_epoch_activation_exit_churn_limit,
        max_bytes,
    )?;
    parameters.electra.fork_version = config_version_override(
        case,
        "ELECTRA_FORK_VERSION",
        parameters.electra.fork_version,
        max_bytes,
    )?;
    parameters.fork_epoch =
        config_override(case, "FULU_FORK_EPOCH", parameters.fork_epoch, max_bytes)?;
    parameters.electra_fork_epoch =
        config_override(case, "ELECTRA_FORK_EPOCH", parameters.electra_fork_epoch, max_bytes)?;
    if let Some(schedule) = config_value_override(case, "BLOB_SCHEDULE", max_bytes)? {
        let entries = schedule.as_sequence().ok_or("BLOB_SCHEDULE must be a sequence")?;
        if entries.len() > 1024 {
            return Err("BLOB_SCHEDULE exceeds fixture resource limit".into())
        }
        parameters.blob_schedule = entries
            .iter()
            .map(|entry| {
                Ok(fulu_processing::BlobParameters {
                    epoch: entry["EPOCH"].as_u64().ok_or("BLOB_SCHEDULE epoch must be unsigned")?,
                    max_blobs_per_block: entry["MAX_BLOBS_PER_BLOCK"]
                        .as_u64()
                        .ok_or("BLOB_SCHEDULE blob count must be unsigned")?,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
    }
    parameters.validate().map_err(|error| error.to_string())?;
    let mut state = if case.runner == "fork" {
        let pre =
            electra::BeaconState::from_ssz(&bytes, preset).map_err(|error| error.to_string())?;
        fulu_processing::upgrade_to_fulu(&pre, &parameters).map_err(|error| error.to_string())?
    } else {
        fulu::BeaconState::from_ssz(&bytes, preset).map_err(|error| error.to_string())?
    };
    macro_rules! operation {
        ($handler:ident, $value:expr $(,)?) => {
            fulu_processing::$handler(&mut state, $value, &parameters)
        };
    }
    let result: Result<(), String> = if case.runner == "operations" &&
        case.handler == "execution_payload"
    {
        let body = fulu::BeaconBlockBody::from_ssz(
            &read_snappy_ssz(&case.path.join("body.ssz_snappy"), max_bytes)?,
            preset,
        )
        .map_err(|error| error.to_string())?;
        let engine = FixtureExecutionEngine::from_case(case, max_bytes, true)?;
        poll_fixture_future(fulu_processing::process_execution_payload(
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
            "block_header" => fulu_processing::process_block_header(
                &mut state,
                &fulu::BeaconBlock::from_ssz(&operation_bytes, preset)
                    .map_err(|error| error.to_string())?,
                &parameters,
            ),
            "attestation" => {
                let value = fulu::Attestation::from_ssz(&operation_bytes, preset)
                    .map_err(|error| error.to_string())?;
                if spec_only {
                    fulu_processing::spec_only::process_attestation(&mut state, &value, &parameters)
                } else {
                    fulu_processing::process_attestation(&mut state, &value, &parameters)
                }
            }
            "attester_slashing" => operation!(
                process_attester_slashing,
                &fulu::AttesterSlashing::from_ssz(&operation_bytes, preset)
                    .map_err(|error| error.to_string())?,
            ),
            "proposer_slashing" => operation!(
                process_proposer_slashing,
                &fulu::ProposerSlashing::from_ssz(&operation_bytes, preset)
                    .map_err(|error| error.to_string())?,
            ),
            "deposit" => operation!(
                process_deposit,
                &fulu::Deposit::from_ssz(&operation_bytes, preset)
                    .map_err(|error| error.to_string())?,
            ),
            "voluntary_exit" => operation!(
                process_voluntary_exit,
                &fulu::SignedVoluntaryExit::from_ssz(&operation_bytes, preset)
                    .map_err(|error| error.to_string())?,
            ),
            "deposit_request" => operation!(
                process_deposit_request,
                &fulu::DepositRequest::from_ssz(&operation_bytes, preset)
                    .map_err(|error| error.to_string())?
            ),
            "withdrawal_request" => operation!(
                process_withdrawal_request,
                &fulu::WithdrawalRequest::from_ssz(&operation_bytes, preset)
                    .map_err(|error| error.to_string())?
            ),
            "consolidation_request" => operation!(
                process_consolidation_request,
                &fulu::ConsolidationRequest::from_ssz(&operation_bytes, preset)
                    .map_err(|error| error.to_string())?
            ),
            "withdrawals" => operation!(
                process_withdrawals,
                &fulu::ExecutionPayload::from_ssz(&operation_bytes, preset)
                    .map_err(|error| error.to_string())?
            ),
            "bls_to_execution_change" => operation!(
                process_bls_to_execution_change,
                &fulu::SignedBLSToExecutionChange::from_ssz(&operation_bytes, preset)
                    .map_err(|error| error.to_string())?
            ),
            "sync_aggregate" => operation!(
                process_sync_aggregate,
                &fulu::SyncAggregate::from_ssz(&operation_bytes, preset)
                    .map_err(|error| error.to_string())?,
            ),
            _ => return Err("unsupported Fulu operation handler".into()),
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
                fulu::SignedBeaconBlock::from_ssz(&bytes, preset).map_err(|error| error.to_string())
            })
            .collect::<Result<Vec<_>, String>>()?;
        let engine = FixtureExecutionEngine::from_case(case, max_bytes, false)?;
        blocks.iter().try_for_each(|block| {
            poll_fixture_future(fulu_processing::process_signed_block(
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
        fulu_processing::process_slots(&mut state, target, &parameters)
            .map_err(|error| error.to_string())
    } else {
        match case.handler.as_str() {
            "justification_and_finalization" => {
                fulu_processing::process_justification_and_finalization(&mut state, &parameters)
            }
            "rewards_and_penalties" => {
                fulu_processing::process_rewards_and_penalties(&mut state, &parameters)
            }
            "registry_updates" => {
                fulu_processing::process_registry_updates(&mut state, &parameters)
            }
            "slashings" => fulu_processing::process_slashings(&mut state, &parameters),
            "eth1_data_reset" => fulu_processing::process_eth1_data_reset(&mut state, &parameters),
            "effective_balance_updates" => {
                fulu_processing::process_effective_balance_updates(&mut state, &parameters)
            }
            "slashings_reset" => fulu_processing::process_slashings_reset(&mut state, &parameters),
            "randao_mixes_reset" => {
                fulu_processing::process_randao_mixes_reset(&mut state, &parameters)
            }
            "historical_summaries_update" => {
                fulu_processing::process_historical_summaries_update(&mut state, &parameters)
            }
            "participation_flag_updates" => {
                fulu_processing::process_participation_flag_updates(&mut state, &parameters)
            }
            "inactivity_updates" => {
                fulu_processing::process_inactivity_updates(&mut state, &parameters)
            }
            "proposer_lookahead" => {
                fulu_processing::process_proposer_lookahead(&mut state, &parameters)
            }
            "pending_deposits" | "pending_deposits_churn" => {
                fulu_processing::process_pending_deposits(&mut state, &parameters)
            }
            "pending_consolidations" => {
                fulu_processing::process_pending_consolidations(&mut state, &parameters)
            }
            "sync_committee_updates" => {
                fulu_processing::process_sync_committee_updates(&mut state, &parameters)
            }
            _ => return Err("unsupported Fulu processing handler".into()),
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
    let post = reth_embedded_beacon_state_transition::ssz::fulu::BeaconState::from_ssz(
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
