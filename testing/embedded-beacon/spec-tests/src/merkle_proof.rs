//! Published regular collection proofs are derived from complete typed bodies.

use super::{light_client, Case, Outcome};

pub(super) fn supports(case: &Case) -> bool {
    case.runner == "merkle_proof" &&
        case.handler == "single_merkle_proof" &&
        case.suite == "BeaconBlockBody" &&
        matches!(case.fork.as_str(), "deneb" | "electra" | "fulu") &&
        matches!(case.preset.as_str(), "mainnet" | "minimal")
}

pub(super) fn run_case(case: &Case, max_bytes: usize) -> Result<(), (Outcome, String)> {
    light_client::run_merkle(case, max_bytes).map_err(|error| (Outcome::Failed, error))
}
