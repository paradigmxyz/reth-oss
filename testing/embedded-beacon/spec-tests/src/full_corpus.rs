//! Fresh full-corpus acceptance for both independently authenticated source directories.
//!
//! The expected identities are derived from the pinned original primary and compliance manifests.
//! Case paths are unique and sorted; input files are qualified by the fixed source role order.
//! Domain-separated SHA-256 encodings use UTF-8 strings prefixed by u64 little-endian byte lengths.
//! Input tuples append the decoded original 32-byte checksum after the role and file path.

use super::{
    authenticated_cases, check_full_matrix, check_required_suites, empty_report, execute_cases,
    hex, FixtureManifest, Report, RunOptions, SPEC_RELEASE,
};
use reth_embedded_beacon_primitives::root_from_hex;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::Path;

/// Exact primary manifest bytes used to derive the full source and case identities.
pub const PRIMARY_MANIFEST_SHA256: &str =
    "0x513216f9ca4c07092e35dff6a6ce6eab383823c36dd232949d7a839192f7bc0f";
/// Exact compliance manifest bytes used to derive the full source and case identities.
pub const COMPLIANCE_MANIFEST_SHA256: &str =
    "0xe7fd977bcec49da3533634bdbf7a4ad9295d7bcfdcaa0efcad63d452c32f17cc";

const FULL_CASE_COUNT: usize = 73_992;
const FULL_CASE_IDS_SHA256: &str =
    "0xa84ceee4fc3a97fe266dce31068e0f4d16706036d9d12284590f7d31c1b6762c";
const FULL_INPUT_FILES_SHA256: &str =
    "0x3c9645abcebcec33616e82c0731c1ce15f710bb115515dcaf938c063319f24d7";
const SOURCE_COUNTS: [(&str, usize, usize); 2] =
    [("primary", 329_206, 63_744), ("compliance", 967_756, 10_248)];

/// Original source members independently authenticated before any case execution.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CorpusAuthentication {
    /// Fixed source role: primary or compliance.
    pub role: String,
    /// Every original file checked against its exact expected SHA-256.
    pub files: usize,
    /// Discovered original cases in this source.
    pub cases: usize,
}

/// Input identities from one fresh complete invocation, never an older report union.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FullCorpusEvidence {
    /// SHA-256 of the sorted unique canonical case IDs.
    pub case_ids_sha256: String,
    /// SHA-256 of role-qualified sorted original file-path/checksum tuples.
    pub input_files_sha256: String,
    /// Authentication counts in the fixed primary, compliance source order.
    pub corpora: Vec<CorpusAuthentication>,
}

/// Authenticate both exact original corpora, then execute their sorted unique union once.
/// Any inventory, checksum, source-identity or matrix error prevents all case execution.
/// This establishes published fixture conformance only; runtime synchronization remains separate.
pub fn run_complete(
    primary_root: &Path,
    primary: &FixtureManifest,
    compliance_root: &Path,
    compliance: &FixtureManifest,
    options: &RunOptions,
) -> Report {
    let mut report = empty_report(true);
    if options.focused {
        report.errors.push("complete corpus runs cannot be focused".into());
        return report;
    }
    let mut cases = Vec::with_capacity(FULL_CASE_COUNT);
    let mut corpora = Vec::with_capacity(2);
    for (role, root, manifest) in
        [("primary", primary_root, primary), ("compliance", compliance_root, compliance)]
    {
        if options.progress_every != 0 {
            eprintln!("fixture source: authenticating {role} corpus");
        }
        let source_cases = match authenticated_cases(root, manifest, options) {
            Ok(cases) => cases,
            Err(error) => {
                report.errors.push(format!("{role} fixture setup: {error}"));
                return report;
            }
        };
        check_required_suites(&source_cases, manifest, &mut report.errors);
        corpora.push(CorpusAuthentication {
            role: role.into(),
            files: manifest.files.len(),
            cases: source_cases.len(),
        });
        cases.extend(source_cases);
    }
    cases.sort_unstable_by(|left, right| left.relative.cmp(&right.relative));
    if let Some(duplicate) = cases.windows(2).find(|pair| pair[0].relative == pair[1].relative) {
        report.errors.push(format!("duplicate canonical fixture case: {}", duplicate[0].relative));
        return report;
    }
    let input_files_sha256 = match input_files_digest(primary, compliance) {
        Ok(digest) => digest,
        Err(error) => {
            report.errors.push(error);
            return report;
        }
    };
    let case_ids_sha256 = sorted_case_ids_digest(cases.iter().map(|case| case.relative.as_str()));
    if cases.len() != FULL_CASE_COUNT || case_ids_sha256 != FULL_CASE_IDS_SHA256 {
        report.errors.push(format!(
            "complete fixture case identity differs from the pinned {FULL_CASE_COUNT}-case corpus"
        ));
    }
    if input_files_sha256 != FULL_INPUT_FILES_SHA256 || !source_counts_match(&corpora) {
        report
            .errors
            .push("complete fixture input identity differs from pinned source corpora".into());
    }
    check_full_matrix(&cases, &mut report.errors);
    report.full_corpus = Some(FullCorpusEvidence { case_ids_sha256, input_files_sha256, corpora });
    if report.errors.is_empty() {
        execute_cases(&mut report, cases, options);
    }
    report
}

pub(super) fn report_is_complete(report: &Report) -> bool {
    if !report.complete_matrix ||
        report.spec_release != SPEC_RELEASE ||
        report.cases.len() != FULL_CASE_COUNT ||
        !report.passed()
    {
        return false;
    }
    let Some(evidence) = &report.full_corpus else { return false };
    if evidence.case_ids_sha256 != FULL_CASE_IDS_SHA256 ||
        evidence.input_files_sha256 != FULL_INPUT_FILES_SHA256 ||
        !source_counts_match(&evidence.corpora)
    {
        return false;
    }
    // Recompute the terminal case-set identity; serialized evidence alone cannot hide a missing,
    // duplicate or substituted result. Older reports have no authenticated full-source evidence.
    let mut paths = report.cases.iter().map(|case| case.path.as_str()).collect::<Vec<_>>();
    paths.sort_unstable();
    !paths.windows(2).any(|pair| pair[0] == pair[1]) &&
        sorted_case_ids_digest(paths) == FULL_CASE_IDS_SHA256
}

fn source_counts_match(corpora: &[CorpusAuthentication]) -> bool {
    corpora.len() == SOURCE_COUNTS.len() &&
        corpora.iter().zip(SOURCE_COUNTS).all(|(actual, (role, files, cases))| {
            actual.role == role && actual.files == files && actual.cases == cases
        })
}

pub(super) fn sorted_case_ids_digest<'a>(paths: impl IntoIterator<Item = &'a str>) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"embedded-beacon-full-case-ids-v1\0");
    for relative in paths {
        sized(&mut hasher, relative.as_bytes());
    }
    hex(&hasher.finalize())
}

pub(super) fn input_files_digest(
    primary: &FixtureManifest,
    compliance: &FixtureManifest,
) -> Result<String, String> {
    let mut hasher = Sha256::new();
    hasher.update(b"embedded-beacon-full-input-files-v1\0");
    for (role, manifest) in [("primary", primary), ("compliance", compliance)] {
        for (relative, expected) in &manifest.files {
            sized(&mut hasher, role.as_bytes());
            sized(&mut hasher, relative.as_bytes());
            hasher.update(
                root_from_hex(expected)
                    .map_err(|error| format!("invalid {role} checksum for {relative}: {error}"))?,
            );
        }
    }
    Ok(hex(&hasher.finalize()))
}

fn sized(hasher: &mut Sha256, value: &[u8]) {
    hasher.update((value.len() as u64).to_le_bytes());
    hasher.update(value);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{inventory, CaseResult, Outcome};
    use std::collections::BTreeMap;
    use tempfile::TempDir;

    fn corpus(name: &str) -> (TempDir, FixtureManifest) {
        let directory = TempDir::new().unwrap();
        let case =
            directory.path().join(format!("general/phase0/ssz_generic/boolean/invalid/{name}"));
        reth_fs_util::create_dir_all(&case).unwrap();
        reth_fs_util::write(
            case.join("serialized.ssz_snappy"),
            snap::raw::Encoder::new().compress_vec(&[2]).unwrap(),
        )
        .unwrap();
        let manifest = inventory(directory.path()).unwrap();
        (directory, manifest)
    }

    #[test]
    fn every_source_is_authenticated_before_any_handler_executes() {
        let (primary_root, primary) = corpus("primary");
        let (compliance_root, compliance) = corpus("compliance");
        let relative = compliance.files.keys().next().unwrap();
        reth_fs_util::write(compliance_root.path().join(relative), b"corrupt").unwrap();
        let report = run_complete(
            primary_root.path(),
            &primary,
            compliance_root.path(),
            &compliance,
            &RunOptions::default(),
        );
        assert_eq!(
            report.errors,
            vec![format!("compliance fixture setup: fixture checksum mismatch: {relative}")]
        );
        assert!(report.cases.is_empty());
        assert!(report.full_corpus.is_none());
        assert!(!report.conformance_passed());
    }

    #[test]
    fn duplicate_original_case_ids_are_rejected_before_execution() {
        let (primary_root, primary) = corpus("same");
        let (compliance_root, compliance) = corpus("same");
        let report = run_complete(
            primary_root.path(),
            &primary,
            compliance_root.path(),
            &compliance,
            &RunOptions::default(),
        );
        assert_eq!(
            report.errors,
            vec![
                "duplicate canonical fixture case: general/phase0/ssz_generic/boolean/invalid/same"
            ]
        );
        assert!(report.cases.is_empty());
        assert!(!report.conformance_passed());
    }

    #[test]
    fn passing_local_subsets_and_legacy_reports_cannot_claim_full_conformance() {
        let (primary_root, primary) = corpus("primary");
        let (compliance_root, compliance) = corpus("compliance");
        let report = run_complete(
            primary_root.path(),
            &primary,
            compliance_root.path(),
            &compliance,
            &RunOptions::default(),
        );
        assert!(!report.errors.is_empty());
        assert!(report.cases.is_empty());
        assert!(!report.conformance_passed());
        let mut legacy = empty_report(true);
        legacy.cases.push(CaseResult {
            path: "invented".into(),
            outcome: Outcome::Passed,
            message: String::new(),
            seconds: 0.0,
            spec_only_bls_disabled: false,
            spec_only_execution_mock: false,
            spec_only_data_availability_mock: false,
            spec_only_asserted_graph: false,
        });
        let mut encoded = serde_json::to_value(&legacy).unwrap();
        encoded.as_object_mut().unwrap().remove("full_corpus");
        let decoded = serde_json::from_value::<Report>(encoded).unwrap();
        assert!(decoded.passed());
        assert!(decoded.full_corpus.is_none());
        assert!(!decoded.conformance_passed());
    }

    #[test]
    fn domains_lengths_and_source_roles_bind_ambiguous_paths() {
        assert_eq!(
            sorted_case_ids_digest(["ab", "c"]),
            "0x37cf853c87720cc9b925621fa2e9d8fe372ee03fe681c4971817e9848dcc3cc6"
        );
        assert_ne!(sorted_case_ids_digest(["ab", "c"]), sorted_case_ids_digest(["a", "bc"]));
        let mut primary = FixtureManifest {
            spec_release: SPEC_RELEASE.into(),
            required_suites: vec![],
            files: BTreeMap::from([("config.yaml".into(), "00".repeat(32))]),
        };
        let mut compliance = FixtureManifest {
            spec_release: SPEC_RELEASE.into(),
            required_suites: vec![],
            files: BTreeMap::from([("config.yaml".into(), "11".repeat(32))]),
        };
        let digest = input_files_digest(&primary, &compliance).unwrap();
        assert_eq!(digest, "0x7f74ed15e61a32fd45d1b9becc91cad7166585765017569cb333150c84d04813");
        std::mem::swap(&mut primary, &mut compliance);
        assert_ne!(digest, input_files_digest(&primary, &compliance).unwrap());
    }

    #[test]
    fn full_gate_requires_every_unique_passing_original_case_identity() {
        // These synthetic outcomes test acceptance policy only. They are never published fixture
        // execution evidence and never enter the authenticated complete invocation above.
        let encoded = include_bytes!("../testdata/full-corpus-case-ids.txt.snappy");
        assert_eq!(snap::raw::decompress_len(encoded).unwrap(), 5_570_086);
        let decoded = snap::raw::Decoder::new().decompress_vec(encoded).unwrap();
        assert_eq!(
            hex(&Sha256::digest(&decoded)),
            "0xa9346b27d0c013d17016950ddf60364f22d0d51ef7111b767d61e90e5f5ded14"
        );
        let identities = std::str::from_utf8(&decoded).unwrap();
        let mut report = empty_report(true);
        report.cases = identities
            .lines()
            .map(|path| CaseResult {
                path: path.into(),
                outcome: Outcome::Passed,
                message: String::new(),
                seconds: 0.0,
                spec_only_bls_disabled: true,
                spec_only_execution_mock: true,
                spec_only_data_availability_mock: true,
                spec_only_asserted_graph: true,
            })
            .collect();
        report.full_corpus = Some(FullCorpusEvidence {
            case_ids_sha256: FULL_CASE_IDS_SHA256.into(),
            input_files_sha256: FULL_INPUT_FILES_SHA256.into(),
            corpora: SOURCE_COUNTS
                .into_iter()
                .map(|(role, files, cases)| CorpusAuthentication {
                    role: role.into(),
                    files,
                    cases,
                })
                .collect(),
        });
        assert_eq!(report.cases.len(), FULL_CASE_COUNT);
        assert!(report.conformance_passed());
        for outcome in [Outcome::Failed, Outcome::Unsupported] {
            report.cases[0].outcome = outcome;
            assert!(!report.conformance_passed());
        }
        report.cases[0].outcome = Outcome::Passed;
        report.errors.push("worker infrastructure failure".into());
        assert!(!report.conformance_passed());
        report.errors.clear();
        let missing = report.cases.pop().unwrap();
        assert!(!report.conformance_passed());
        report.cases.push(missing);
        let original_path = report.cases[0].path.clone();
        report.cases[0].path = report.cases[1].path.clone();
        assert!(!report.conformance_passed());
        report.cases[0].path = "substituted".into();
        assert!(!report.conformance_passed());
        report.cases[0].path = original_path;
        report.complete_matrix = false;
        assert!(!report.conformance_passed());
        report.complete_matrix = true;
        report.spec_release = "master".into();
        assert!(!report.conformance_passed());
        report.spec_release = SPEC_RELEASE.into();
        let evidence = report.full_corpus.take().unwrap();
        assert!(!report.conformance_passed());
        report.full_corpus = Some(evidence);
        report.full_corpus.as_mut().unwrap().input_files_sha256 = "0x00".into();
        assert!(!report.conformance_passed());
        report.full_corpus.as_mut().unwrap().input_files_sha256 = FULL_INPUT_FILES_SHA256.into();
        report.full_corpus.as_mut().unwrap().corpora.reverse();
        assert!(!report.conformance_passed());
    }
}
