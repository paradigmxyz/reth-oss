//! Published finality and randomized sequences use the same complete signed-block transitions.
//! Original fixture identities are checked before this adapter; all post-state bytes remain exact.

use super::{
    run_altair_processing, run_bellatrix_processing, run_capella_processing, run_deneb_processing,
    run_electra_processing, run_fulu_processing, run_gloas_processing, run_phase0_processing, Case,
    Outcome,
};

pub(super) fn supports(case: &Case) -> bool {
    matches!(
        (case.runner.as_str(), case.handler.as_str()),
        ("finality", "finality") | ("random", "random")
    ) && super::FORKS.contains(&case.fork.as_str())
}

pub(super) fn uses_execution_mock(case: &Case) -> bool {
    supports(case) &&
        matches!(case.fork.as_str(), "bellatrix" | "capella" | "deneb" | "electra" | "fulu")
}

pub(super) fn run_case(case: &Case, max_bytes: usize) -> Result<(), (Outcome, String)> {
    let mut sequence = case.clone();
    // These published formats contain pre/post and blocks_count/blocks_N, like sanity/blocks.
    // Reuse only that execution path, after validating the actual source manifest identity.
    sequence.runner = "sanity".into();
    sequence.handler = "blocks".into();
    let result = match case.fork.as_str() {
        "phase0" => run_phase0_processing(&sequence, max_bytes),
        "altair" => run_altair_processing(&sequence, max_bytes),
        "bellatrix" => run_bellatrix_processing(&sequence, max_bytes),
        "capella" => run_capella_processing(&sequence, max_bytes),
        "deneb" => run_deneb_processing(&sequence, max_bytes),
        "electra" => run_electra_processing(&sequence, max_bytes),
        "fulu" => run_fulu_processing(&sequence, max_bytes),
        "gloas" => run_gloas_processing(&sequence, max_bytes),
        _ => return Err((Outcome::Unsupported, "unknown sequence fork".into())),
    };
    result.map_err(|error| (Outcome::Failed, error))
}
