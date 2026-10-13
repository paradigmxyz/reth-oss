//! Published phase0 processing fixture adapter.

use super::{
    fixture_disables_bls, hex, phase0, phase0_processing, read_snappy_ssz, schema_preset, yaml,
    Case,
};

pub(super) fn run_phase0_processing(case: &Case, max_bytes: usize) -> Result<(), String> {
    let preset = schema_preset(case)?;
    let spec_only = fixture_disables_bls(case, max_bytes)?;
    let bytes = read_snappy_ssz(&case.path.join("pre.ssz_snappy"), max_bytes)?;
    let mut state =
        reth_embedded_beacon_state_transition::ssz::phase0::BeaconState::from_ssz(&bytes, preset)
            .map_err(|error| error.to_string())?;
    let mut parameters = phase0_processing::Phase0Parameters::for_preset(preset);
    super::fixture_config::apply_phase0(case, max_bytes, &mut parameters)?;
    parameters.validate().map_err(|error| error.to_string())?;
    macro_rules! operation {
        ($handler:ident, $value:expr $(,)?) => {
            if spec_only {
                phase0_processing::spec_only::$handler(&mut state, $value, &parameters)
            } else {
                phase0_processing::$handler(&mut state, $value, &parameters)
            }
        };
    }
    let result = if case.runner == "operations" {
        let operation_bytes = read_snappy_ssz(
            &case.path.join(format!(
                "{}.ssz_snappy",
                if case.handler == "block_header" { "block" } else { &case.handler }
            )),
            max_bytes,
        )?;
        match case.handler.as_str() {
            "block_header" => phase0_processing::process_block_header(
                &mut state,
                &phase0::BeaconBlock::from_ssz(&operation_bytes, preset)
                    .map_err(|error| error.to_string())?,
                &parameters,
            ),
            "attestation" => operation!(
                process_attestation,
                &phase0::Attestation::from_ssz(&operation_bytes, preset)
                    .map_err(|error| error.to_string())?,
            ),
            "attester_slashing" => operation!(
                process_attester_slashing,
                &phase0::AttesterSlashing::from_ssz(&operation_bytes, preset)
                    .map_err(|error| error.to_string())?,
            ),
            "proposer_slashing" => operation!(
                process_proposer_slashing,
                &phase0::ProposerSlashing::from_ssz(&operation_bytes, preset)
                    .map_err(|error| error.to_string())?,
            ),
            "deposit" => operation!(
                process_deposit,
                &phase0::Deposit::from_ssz(&operation_bytes, preset)
                    .map_err(|error| error.to_string())?,
            ),
            "voluntary_exit" => operation!(
                process_voluntary_exit,
                &phase0::SignedVoluntaryExit::from_ssz(&operation_bytes, preset)
                    .map_err(|error| error.to_string())?,
            ),
            _ => return Err("unsupported Phase0 operation handler".into()),
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
                phase0::SignedBeaconBlock::from_ssz(&bytes, preset)
                    .map_err(|error| error.to_string())
            })
            .collect::<Result<Vec<_>, String>>()?;
        blocks.iter().try_for_each(|block| {
            if spec_only {
                phase0_processing::spec_only::state_transition(&mut state, block, &parameters)
            } else {
                phase0_processing::process_signed_block(&mut state, block, &parameters)
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
        phase0_processing::process_slots(&mut state, target, &parameters)
    } else {
        match case.handler.as_str() {
            "justification_and_finalization" => {
                phase0_processing::process_justification_and_finalization(&mut state, &parameters)
            }
            "rewards_and_penalties" => {
                phase0_processing::process_rewards_and_penalties(&mut state, &parameters)
            }
            "registry_updates" => {
                phase0_processing::process_registry_updates(&mut state, &parameters)
            }
            "slashings" => phase0_processing::process_slashings(&mut state, &parameters),
            "eth1_data_reset" => {
                phase0_processing::process_eth1_data_reset(&mut state, &parameters)
            }
            "effective_balance_updates" => {
                phase0_processing::process_effective_balance_updates(&mut state, &parameters)
            }
            "slashings_reset" => {
                phase0_processing::process_slashings_reset(&mut state, &parameters)
            }
            "randao_mixes_reset" => {
                phase0_processing::process_randao_mixes_reset(&mut state, &parameters)
            }
            "historical_roots_update" => {
                phase0_processing::process_historical_roots_update(&mut state, &parameters)
            }
            "participation_record_updates" => {
                phase0_processing::process_participation_record_updates(&mut state, &parameters)
            }
            _ => return Err("unsupported Phase0 processing handler".into()),
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
    let post = reth_embedded_beacon_state_transition::ssz::phase0::BeaconState::from_ssz(
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
