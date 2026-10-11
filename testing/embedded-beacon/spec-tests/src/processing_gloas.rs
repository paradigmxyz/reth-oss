//! Published gloas processing fixture adapter.

use super::{
    config_override, config_value_override, config_version_override, fixture_disables_bls, fulu,
    fulu_processing, gloas, gloas_processing, hex, read_snappy_ssz, schema_preset, yaml, Case,
    SszPreset,
};

pub(super) fn fixture_gloas_parameters(
    case: &Case,
    preset: SszPreset,
    max_bytes: usize,
) -> Result<gloas_processing::GloasParameters, String> {
    let mut parameters = gloas_processing::GloasParameters::for_preset(preset);
    parameters.fulu.electra.deneb.capella.bellatrix.altair.phase0.ejection_balance =
        config_override(
            case,
            "EJECTION_BALANCE",
            parameters.fulu.electra.deneb.capella.bellatrix.altair.phase0.ejection_balance,
            max_bytes,
        )?;
    parameters.fulu.electra.deneb.capella.bellatrix.altair.phase0.min_per_epoch_churn_limit =
        config_override(
            case,
            "MIN_PER_EPOCH_CHURN_LIMIT",
            parameters.fulu.electra.deneb.capella.bellatrix.altair.phase0.min_per_epoch_churn_limit,
            max_bytes,
        )?;
    parameters.fulu.electra.deneb.capella.bellatrix.altair.phase0.churn_limit_quotient =
        config_override(
            case,
            "CHURN_LIMIT_QUOTIENT",
            parameters.fulu.electra.deneb.capella.bellatrix.altair.phase0.churn_limit_quotient,
            max_bytes,
        )?;
    parameters
        .fulu
        .electra
        .deneb
        .capella
        .bellatrix
        .altair
        .phase0
        .min_validator_withdrawability_delay = config_override(
        case,
        "MIN_VALIDATOR_WITHDRAWABILITY_DELAY",
        parameters
            .fulu
            .electra
            .deneb
            .capella
            .bellatrix
            .altair
            .phase0
            .min_validator_withdrawability_delay,
        max_bytes,
    )?;
    parameters.fulu.electra.deneb.capella.bellatrix.altair.phase0.shard_committee_period =
        config_override(
            case,
            "SHARD_COMMITTEE_PERIOD",
            parameters.fulu.electra.deneb.capella.bellatrix.altair.phase0.shard_committee_period,
            max_bytes,
        )?;
    parameters.fulu.electra.deneb.capella.bellatrix.altair.phase0.genesis_fork_version =
        config_version_override(
            case,
            "GENESIS_FORK_VERSION",
            parameters.fulu.electra.deneb.capella.bellatrix.altair.phase0.genesis_fork_version,
            max_bytes,
        )?;
    parameters.fork_version =
        config_version_override(case, "GLOAS_FORK_VERSION", parameters.fork_version, max_bytes)?;
    parameters.fulu.electra.deneb.capella.bellatrix.altair.inactivity_score_bias = config_override(
        case,
        "INACTIVITY_SCORE_BIAS",
        parameters.fulu.electra.deneb.capella.bellatrix.altair.inactivity_score_bias,
        max_bytes,
    )?;
    parameters.fulu.electra.deneb.capella.bellatrix.altair.inactivity_score_recovery_rate =
        config_override(
            case,
            "INACTIVITY_SCORE_RECOVERY_RATE",
            parameters.fulu.electra.deneb.capella.bellatrix.altair.inactivity_score_recovery_rate,
            max_bytes,
        )?;
    parameters.fulu.electra.deneb.capella.bellatrix.seconds_per_slot = config_override(
        case,
        "SECONDS_PER_SLOT",
        parameters.fulu.electra.deneb.capella.bellatrix.seconds_per_slot,
        max_bytes,
    )?;
    let duration = config_override(
        case,
        "SLOT_DURATION_MS",
        parameters
            .fulu
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
    parameters.fulu.electra.deneb.capella.bellatrix.seconds_per_slot = duration / 1000;
    parameters.fulu.electra.deneb.capella.bellatrix.altair.fork_version = config_version_override(
        case,
        "ALTAIR_FORK_VERSION",
        parameters.fulu.electra.deneb.capella.bellatrix.altair.fork_version,
        max_bytes,
    )?;
    parameters.fulu.electra.deneb.capella.bellatrix.fork_version = config_version_override(
        case,
        "BELLATRIX_FORK_VERSION",
        parameters.fulu.electra.deneb.capella.bellatrix.fork_version,
        max_bytes,
    )?;
    parameters.fulu.electra.deneb.capella.fork_version = config_version_override(
        case,
        "CAPELLA_FORK_VERSION",
        parameters.fulu.electra.deneb.capella.fork_version,
        max_bytes,
    )?;
    parameters.fulu.electra.max_blobs_per_block = config_override(
        case,
        "MAX_BLOBS_PER_BLOCK_ELECTRA",
        parameters.fulu.electra.max_blobs_per_block,
        max_bytes,
    )?;
    parameters.fulu.electra.deneb.max_per_epoch_activation_churn_limit = config_override(
        case,
        "MAX_PER_EPOCH_ACTIVATION_CHURN_LIMIT",
        parameters.fulu.electra.deneb.max_per_epoch_activation_churn_limit,
        max_bytes,
    )?;
    parameters.fulu.electra.deneb.fork_version = config_version_override(
        case,
        "DENEB_FORK_VERSION",
        parameters.fulu.electra.deneb.fork_version,
        max_bytes,
    )?;
    parameters.fulu.electra.min_per_epoch_churn_limit = config_override(
        case,
        "MIN_PER_EPOCH_CHURN_LIMIT_ELECTRA",
        parameters.fulu.electra.min_per_epoch_churn_limit,
        max_bytes,
    )?;
    parameters.fulu.electra.max_per_epoch_activation_exit_churn_limit = config_override(
        case,
        "MAX_PER_EPOCH_ACTIVATION_EXIT_CHURN_LIMIT",
        parameters.fulu.electra.max_per_epoch_activation_exit_churn_limit,
        max_bytes,
    )?;
    parameters.fulu.electra.fork_version = config_version_override(
        case,
        "ELECTRA_FORK_VERSION",
        parameters.fulu.electra.fork_version,
        max_bytes,
    )?;
    parameters.fork_epoch =
        config_override(case, "GLOAS_FORK_EPOCH", parameters.fork_epoch, max_bytes)?;
    parameters.fulu.electra_fork_epoch =
        config_override(case, "ELECTRA_FORK_EPOCH", parameters.fulu.electra_fork_epoch, max_bytes)?;
    if let Some(schedule) = config_value_override(case, "BLOB_SCHEDULE", max_bytes)? {
        let entries = schedule.as_sequence().ok_or("BLOB_SCHEDULE must be a sequence")?;
        if entries.len() > 1024 {
            return Err("BLOB_SCHEDULE exceeds fixture resource limit".into())
        }
        parameters.fulu.blob_schedule = entries
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
    parameters.fulu.fork_version = config_version_override(
        case,
        "FULU_FORK_VERSION",
        parameters.fulu.fork_version,
        max_bytes,
    )?;
    parameters.fulu.fork_epoch =
        config_override(case, "FULU_FORK_EPOCH", parameters.fulu.fork_epoch, max_bytes)?;
    parameters.churn_limit_quotient = config_override(
        case,
        "CHURN_LIMIT_QUOTIENT_GLOAS",
        parameters.churn_limit_quotient,
        max_bytes,
    )?;
    parameters.consolidation_churn_limit_quotient = config_override(
        case,
        "CONSOLIDATION_CHURN_LIMIT_QUOTIENT",
        parameters.consolidation_churn_limit_quotient,
        max_bytes,
    )?;
    parameters.max_per_epoch_activation_churn_limit = config_override(
        case,
        "MAX_PER_EPOCH_ACTIVATION_CHURN_LIMIT_GLOAS",
        parameters.max_per_epoch_activation_churn_limit,
        max_bytes,
    )?;
    parameters.min_builder_withdrawability_delay = config_override(
        case,
        "MIN_BUILDER_WITHDRAWABILITY_DELAY",
        parameters.min_builder_withdrawability_delay,
        max_bytes,
    )?;
    parameters.validate().map_err(|error| error.to_string())?;
    Ok(parameters)
}

pub(super) fn run_gloas_processing(case: &Case, max_bytes: usize) -> Result<(), String> {
    let preset = schema_preset(case)?;
    let spec_only = fixture_disables_bls(case, max_bytes)?;
    let bytes = read_snappy_ssz(&case.path.join("pre.ssz_snappy"), max_bytes)?;
    let parameters = fixture_gloas_parameters(case, preset, max_bytes)?;
    let mut state = if case.runner == "fork" {
        let pre = fulu::BeaconState::from_ssz(&bytes, preset).map_err(|error| error.to_string())?;
        gloas_processing::upgrade_to_gloas(&pre, &parameters).map_err(|error| error.to_string())?
    } else {
        gloas::BeaconState::from_ssz(&bytes, preset).map_err(|error| error.to_string())?
    };
    macro_rules! operation {
        ($handler:ident, $value:expr $(,)?) => {
            gloas_processing::$handler(&mut state, $value, &parameters)
        };
    }
    let result: Result<(), String> = if case.runner == "fork" {
        Ok(())
    } else if case.runner == "operations" && case.handler == "withdrawals" {
        gloas_processing::process_withdrawals(&mut state, &parameters)
            .map_err(|error| error.to_string())
    } else if case.runner == "operations" {
        let operation_bytes = read_snappy_ssz(
            &case.path.join(format!(
                "{}.ssz_snappy",
                if matches!(case.handler.as_str(), "block_header" | "parent_execution_payload") {
                    "block"
                } else if case.handler == "voluntary_exit_churn" {
                    "voluntary_exit"
                } else if case.handler == "bls_to_execution_change" {
                    "address_change"
                } else {
                    &case.handler
                }
            )),
            max_bytes,
        )?;
        match case.handler.as_str() {
            "block_header" => gloas_processing::process_block_header(
                &mut state,
                &gloas::BeaconBlock::from_ssz(&operation_bytes, preset)
                    .map_err(|error| error.to_string())?,
                &parameters,
            ),
            "attestation" => {
                let value = gloas::Attestation::from_ssz(&operation_bytes, preset)
                    .map_err(|error| error.to_string())?;
                let parent_slot = yaml(&case.path.join("meta.yaml"), max_bytes)?["parent_slot"]
                    .as_u64()
                    .ok_or("Gloas attestation parent_slot must be unsigned")?;
                if spec_only {
                    gloas_processing::spec_only::process_attestation(
                        &mut state,
                        &value,
                        parent_slot,
                        &parameters,
                    )
                } else {
                    gloas_processing::process_attestation(
                        &mut state,
                        &value,
                        parent_slot,
                        &parameters,
                    )
                }
            }
            "attester_slashing" => operation!(
                process_attester_slashing,
                &gloas::AttesterSlashing::from_ssz(&operation_bytes, preset)
                    .map_err(|error| error.to_string())?,
            ),
            "proposer_slashing" => operation!(
                process_proposer_slashing,
                &gloas::ProposerSlashing::from_ssz(&operation_bytes, preset)
                    .map_err(|error| error.to_string())?,
            ),
            "voluntary_exit" | "voluntary_exit_churn" => operation!(
                process_voluntary_exit,
                &gloas::SignedVoluntaryExit::from_ssz(&operation_bytes, preset)
                    .map_err(|error| error.to_string())?,
            ),
            "deposit_request" => operation!(
                process_deposit_request,
                &gloas::DepositRequest::from_ssz(&operation_bytes, preset)
                    .map_err(|error| error.to_string())?
            ),
            "withdrawal_request" => operation!(
                process_withdrawal_request,
                &gloas::WithdrawalRequest::from_ssz(&operation_bytes, preset)
                    .map_err(|error| error.to_string())?
            ),
            "consolidation_request" => operation!(
                process_consolidation_request,
                &gloas::ConsolidationRequest::from_ssz(&operation_bytes, preset)
                    .map_err(|error| error.to_string())?
            ),
            "bls_to_execution_change" => operation!(
                process_bls_to_execution_change,
                &gloas::SignedBLSToExecutionChange::from_ssz(&operation_bytes, preset)
                    .map_err(|error| error.to_string())?
            ),
            "builder_deposit_request" => operation!(
                process_builder_deposit_request,
                &gloas::BuilderDepositRequest::from_ssz(&operation_bytes, preset)
                    .map_err(|error| error.to_string())?
            ),
            "builder_exit_request" => operation!(
                process_builder_exit_request,
                &gloas::BuilderExitRequest::from_ssz(&operation_bytes, preset)
                    .map_err(|error| error.to_string())?
            ),
            "execution_payload_bid" => operation!(
                process_execution_payload_bid,
                &gloas::SignedExecutionPayloadBid::from_ssz(&operation_bytes, preset)
                    .map_err(|error| error.to_string())?
            ),
            "parent_execution_payload" => operation!(
                process_parent_execution_payload,
                &gloas::BeaconBlock::from_ssz(&operation_bytes, preset)
                    .map_err(|error| error.to_string())?
            ),
            "payload_attestation" => operation!(
                process_payload_attestation,
                &gloas::PayloadAttestation::from_ssz(&operation_bytes, preset)
                    .map_err(|error| error.to_string())?
            ),
            "sync_aggregate" => operation!(
                process_sync_aggregate,
                &gloas::SyncAggregate::from_ssz(&operation_bytes, preset)
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
                gloas::SignedBeaconBlock::from_ssz(&bytes, preset)
                    .map_err(|error| error.to_string())
            })
            .collect::<Result<Vec<_>, String>>()?;
        blocks.iter().try_for_each(|block| {
            gloas_processing::process_signed_block(&mut state, block, &parameters)
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
        gloas_processing::process_slots(&mut state, target, &parameters)
            .map_err(|error| error.to_string())
    } else {
        match case.handler.as_str() {
            "justification_and_finalization" => {
                gloas_processing::process_justification_and_finalization(&mut state, &parameters)
            }
            "rewards_and_penalties" => {
                gloas_processing::process_rewards_and_penalties(&mut state, &parameters)
            }
            "registry_updates" => {
                gloas_processing::process_registry_updates(&mut state, &parameters)
            }
            "slashings" => gloas_processing::process_slashings(&mut state, &parameters),
            "eth1_data_reset" => gloas_processing::process_eth1_data_reset(&mut state, &parameters),
            "effective_balance_updates" => {
                gloas_processing::process_effective_balance_updates(&mut state, &parameters)
            }
            "slashings_reset" => gloas_processing::process_slashings_reset(&mut state, &parameters),
            "randao_mixes_reset" => {
                gloas_processing::process_randao_mixes_reset(&mut state, &parameters)
            }
            "historical_summaries_update" => {
                gloas_processing::process_historical_summaries_update(&mut state, &parameters)
            }
            "participation_flag_updates" => {
                gloas_processing::process_participation_flag_updates(&mut state, &parameters)
            }
            "inactivity_updates" => {
                gloas_processing::process_inactivity_updates(&mut state, &parameters)
            }
            "builder_pending_payments" => {
                gloas_processing::process_builder_pending_payments(&mut state, &parameters)
            }
            "ptc_window" => gloas_processing::process_ptc_window(&mut state, &parameters),
            "proposer_lookahead" => {
                gloas_processing::process_proposer_lookahead(&mut state, &parameters)
            }
            "pending_deposits" | "pending_deposits_churn" => {
                gloas_processing::process_pending_deposits(&mut state, &parameters)
            }
            "pending_consolidations" => {
                gloas_processing::process_pending_consolidations(&mut state, &parameters)
            }
            "sync_committee_updates" => {
                gloas_processing::process_sync_committee_updates(&mut state, &parameters)
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
    let post = reth_embedded_beacon_state_transition::ssz::gloas::BeaconState::from_ssz(
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
