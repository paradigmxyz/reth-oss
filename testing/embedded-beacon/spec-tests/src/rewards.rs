//! Exact published reward components, evaluated by the production epoch kernels.

use super::{
    config_override, fixture_gloas_parameters, read_snappy_ssz, schema_preset, Case, Outcome,
};
use reth_embedded_beacon_state_transition::{
    altair_processing::AltairParameters,
    bellatrix_processing::BellatrixParameters,
    capella_processing::CapellaParameters,
    deneb_processing::DenebParameters,
    electra_processing::ElectraParameters,
    fulu_processing::FuluParameters,
    phase0_processing::{self, Phase0Parameters, RewardDeltas},
    rewards::{self, ParticipationRewardState},
    ssz::{altair, bellatrix, capella, deneb, electra, fulu, gloas, phase0},
};

pub(super) fn supports(case: &Case) -> bool {
    case.runner == "rewards" &&
        matches!(case.handler.as_str(), "basic" | "inactivity_scores" | "leak" | "random") &&
        super::FORKS.contains(&case.fork.as_str())
}

pub(super) fn run_case(case: &Case, max_bytes: usize) -> Result<(), (Outcome, String)> {
    run_rewards(case, max_bytes).map_err(|error| (Outcome::Failed, error))
}

fn run_rewards(case: &Case, max_bytes: usize) -> Result<(), String> {
    let preset = schema_preset(case)?;
    let bytes = read_snappy_ssz(&case.path.join("pre.ssz_snappy"), max_bytes)?;
    let bias = config_override(case, "INACTIVITY_SCORE_BIAS", 4, max_bytes)?;
    macro_rules! participation_case {
        ($state:path, $parameters:path, $($field:ident).+) => {{
            let state = <$state>::from_ssz(&bytes, preset).map_err(|error| error.to_string())?;
            let mut parameters = <$parameters>::for_preset(preset);
            parameters.$($field).+ = bias;
            participation_components(case, &state, &parameters, max_bytes)
        }};
    }
    match case.fork.as_str() {
        "phase0" => {
            let state =
                phase0::BeaconState::from_ssz(&bytes, preset).map_err(|error| error.to_string())?;
            let parameters = Phase0Parameters::for_preset(preset);
            for (component, result) in [
                ("source", phase0_processing::get_source_deltas(&state, &parameters)),
                ("target", phase0_processing::get_target_deltas(&state, &parameters)),
                ("head", phase0_processing::get_head_deltas(&state, &parameters)),
                (
                    "inclusion_delay",
                    phase0_processing::get_inclusion_delay_deltas(&state, &parameters),
                ),
                (
                    "inactivity_penalty",
                    phase0_processing::get_inactivity_penalty_deltas(&state, &parameters),
                ),
            ] {
                compare_component(
                    case,
                    component,
                    result.map_err(|error| error.to_string())?,
                    max_bytes,
                )?;
            }
            Ok(())
        }
        "altair" => {
            participation_case!(altair::BeaconState, AltairParameters, inactivity_score_bias)
        }
        "bellatrix" => participation_case!(
            bellatrix::BeaconState,
            BellatrixParameters,
            altair.inactivity_score_bias
        ),
        "capella" => participation_case!(
            capella::BeaconState,
            CapellaParameters,
            bellatrix.altair.inactivity_score_bias
        ),
        "deneb" => participation_case!(
            deneb::BeaconState,
            DenebParameters,
            capella.bellatrix.altair.inactivity_score_bias
        ),
        "electra" => participation_case!(
            electra::BeaconState,
            ElectraParameters,
            deneb.capella.bellatrix.altair.inactivity_score_bias
        ),
        "fulu" => participation_case!(
            fulu::BeaconState,
            FuluParameters,
            electra.deneb.capella.bellatrix.altair.inactivity_score_bias
        ),
        "gloas" => {
            let state =
                gloas::BeaconState::from_ssz(&bytes, preset).map_err(|error| error.to_string())?;
            let parameters = fixture_gloas_parameters(case, preset, max_bytes)?;
            participation_components(case, &state, &parameters, max_bytes)
        }
        _ => Err("unsupported rewards fork".into()),
    }
}

fn participation_components<S: ParticipationRewardState>(
    case: &Case,
    state: &S,
    parameters: &S::Parameters,
    max_bytes: usize,
) -> Result<(), String> {
    for (flag, component) in ["source", "target", "head"].into_iter().enumerate() {
        let actual = rewards::get_flag_index_deltas(state, flag, parameters)
            .map_err(|error| error.to_string())?;
        compare_component(case, component, actual, max_bytes)?;
    }
    let actual = rewards::get_inactivity_penalty_deltas(state, parameters)
        .map_err(|error| error.to_string())?;
    compare_component(case, "inactivity_penalty", actual, max_bytes)
}

fn compare_component(
    case: &Case,
    component: &str,
    actual: RewardDeltas,
    max_bytes: usize,
) -> Result<(), String> {
    if actual.penalties.len() != actual.rewards.len() {
        return Err("actual reward and penalty list lengths differ".into());
    }
    let bytes =
        read_snappy_ssz(&case.path.join(format!("{component}_deltas.ssz_snappy")), max_bytes)?;
    let expected = decode_deltas(&bytes, actual.rewards.len())?;
    for (kind, expected, actual) in [
        ("reward", expected.rewards, actual.rewards),
        ("penalty", expected.penalties, actual.penalties),
    ] {
        if let Some((index, (expected, actual))) = expected
            .iter()
            .zip(&actual)
            .enumerate()
            .find(|(_, (expected, actual))| expected != actual)
        {
            return Err(format!("{component} {kind} differs at validator {index}: expected {expected}, received {actual}"));
        }
    }
    Ok(())
}

// Deltas is a container of two variable-length uint64 lists. Require canonical offsets
// and exact registry length for both lists before allocating either expected component.
fn decode_deltas(bytes: &[u8], count: usize) -> Result<RewardDeltas, String> {
    let list_bytes = count.checked_mul(8).ok_or("reward list size overflow")?;
    let second = 8usize.checked_add(list_bytes).ok_or("reward offset overflow")?;
    let total = second.checked_add(list_bytes).ok_or("reward container size overflow")?;
    if bytes.len() != total {
        return Err("reward container length differs from the validator registry".into());
    }
    let first_offset =
        u32::from_le_bytes(bytes[..4].try_into().map_err(|_| "reward first offset")?) as usize;
    let second_offset =
        u32::from_le_bytes(bytes[4..8].try_into().map_err(|_| "reward second offset")?) as usize;
    if first_offset != 8 || second_offset != second {
        return Err("noncanonical reward list offsets".into());
    }
    let list = |input: &[u8]| {
        input
            .chunks_exact(8)
            .map(|chunk| u64::from_le_bytes(chunk.try_into().expect("exact uint64 chunk")))
            .collect()
    };
    Ok(RewardDeltas { rewards: list(&bytes[8..second]), penalties: list(&bytes[second..]) })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn published_deltas_decode_canonical_lists_without_integer_truncation() {
        let bytes = [
            8u32.to_le_bytes().as_slice(),
            24u32.to_le_bytes().as_slice(),
            u64::MAX.to_le_bytes().as_slice(),
            17u64.to_le_bytes().as_slice(),
            0u64.to_le_bytes().as_slice(),
            31u64.to_le_bytes().as_slice(),
        ]
        .concat();
        let deltas = decode_deltas(&bytes, 2).unwrap();
        assert_eq!(deltas.rewards, [u64::MAX, 17]);
        assert_eq!(deltas.penalties, [0, 31]);
        assert_eq!(
            decode_deltas(&bytes, 1).unwrap_err(),
            "reward container length differs from the validator registry"
        );
        let mut altered = bytes;
        altered[..4].copy_from_slice(&12u32.to_le_bytes());
        assert_eq!(decode_deltas(&altered, 2).unwrap_err(), "noncanonical reward list offsets");
    }

    #[test]
    fn malformed_empty_container_and_overflow_are_rejected_before_indexing() {
        assert_eq!(
            decode_deltas(&[], 0).unwrap_err(),
            "reward container length differs from the validator registry"
        );
        assert_eq!(decode_deltas(&[0; 8], 0).unwrap_err(), "noncanonical reward list offsets");
        assert_eq!(decode_deltas(&[], usize::MAX).unwrap_err(), "reward list size overflow");
        let empty = [8u32.to_le_bytes(), 8u32.to_le_bytes()].concat();
        let deltas = decode_deltas(&empty, 0).unwrap();
        assert!(deltas.rewards.is_empty() && deltas.penalties.is_empty());
    }
}
